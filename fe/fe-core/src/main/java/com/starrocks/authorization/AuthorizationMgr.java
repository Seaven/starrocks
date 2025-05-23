// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.authorization;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.system.SystemId;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.RolePrivilegeCollectionInfo;
import com.starrocks.persist.metablock.MapEntryConsumer;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AlterRoleStmt;
import com.starrocks.sql.ast.CreateRoleStmt;
import com.starrocks.sql.ast.DropRoleStmt;
import com.starrocks.sql.ast.GrantPrivilegeStmt;
import com.starrocks.sql.ast.GrantRoleStmt;
import com.starrocks.sql.ast.RevokePrivilegeStmt;
import com.starrocks.sql.ast.RevokeRoleStmt;
import com.starrocks.sql.ast.UserIdentity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class AuthorizationMgr {
    private static final Logger LOG = LogManager.getLogger(AuthorizationMgr.class);

    @SerializedName(value = "r")
    private Map<String, Long> roleNameToId;
    @SerializedName(value = "i")
    private short pluginId;
    @SerializedName(value = "v")
    private short pluginVersion;

    protected AuthorizationProvider provider;

    protected Map<UserIdentity, UserPrivilegeCollectionV2> userToPrivilegeCollection;
    protected Map<Long, RolePrivilegeCollectionV2> roleIdToPrivilegeCollection;

    private static final int MAX_NUM_CACHED_MERGED_PRIVILEGE_COLLECTION = 1000;
    private static final int CACHED_MERGED_PRIVILEGE_COLLECTION_EXPIRE_MIN = 60;
    protected LoadingCache<UserPrivKey, PrivilegeCollectionV2> ctxToMergedPrivilegeCollections =
            CacheBuilder.newBuilder()
                    .maximumSize(MAX_NUM_CACHED_MERGED_PRIVILEGE_COLLECTION)
                    .expireAfterAccess(CACHED_MERGED_PRIVILEGE_COLLECTION_EXPIRE_MIN, TimeUnit.MINUTES)
                    .build(new CacheLoader<>() {
                        @NotNull
                        @Override
                        public PrivilegeCollectionV2 load(@NotNull UserPrivKey userPrivKey)
                                throws Exception {
                            return loadPrivilegeCollection(userPrivKey.userIdentity, userPrivKey.groups, userPrivKey.roleIds);
                        }
                    });

    public static class UserPrivKey {
        public UserIdentity userIdentity;
        public Set<String> groups;
        public Set<Long> roleIds;

        public UserPrivKey(UserIdentity userIdentity, Set<String> groups, Set<Long> roleIds) {
            this.userIdentity = userIdentity;
            this.groups = groups;
            this.roleIds = roleIds;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            UserPrivKey that = (UserPrivKey) o;
            return Objects.equals(userIdentity, that.userIdentity) && Objects.equals(groups, that.groups) &&
                    Objects.equals(roleIds, that.roleIds);
        }

        @Override
        public int hashCode() {
            return Objects.hash(userIdentity, groups, roleIds);
        }
    }

    private final ReentrantReadWriteLock userLock;
    private final ReentrantReadWriteLock roleLock;

    // set by load() to distinguish brand-new environment with upgraded environment
    private boolean isLoaded = false;

    // only used in deserialization
    protected AuthorizationMgr() {
        roleNameToId = new HashMap<>();
        userToPrivilegeCollection = new HashMap<>();
        roleIdToPrivilegeCollection = new HashMap<>();
        userLock = new ReentrantReadWriteLock();
        roleLock = new ReentrantReadWriteLock();
    }

    public AuthorizationMgr(AuthorizationProvider provider) {
        this.provider = provider;
        pluginId = this.provider.getPluginId();
        pluginVersion = this.provider.getPluginVersion();
        roleNameToId = new HashMap<>();
        userLock = new ReentrantReadWriteLock();
        roleLock = new ReentrantReadWriteLock();
        userToPrivilegeCollection = new HashMap<>();
        roleIdToPrivilegeCollection = new HashMap<>();
        initBuiltinRolesAndUsers();
    }

    public void initBuiltinRolesAndUsers() {
        try {
            // built-in role ids are hard-coded negative numbers
            // because globalStateMgr.getNextId() cannot be called by a follower
            // 1. builtin root role
            RolePrivilegeCollectionV2 rolePrivilegeCollection =
                    initBuiltinRoleUnlocked(PrivilegeBuiltinConstants.ROOT_ROLE_ID,
                            PrivilegeBuiltinConstants.ROOT_ROLE_NAME,
                            "built-in root role which has all privileges on all objects");
            // GRANT ALL ON ALL
            List<ObjectType> objectTypes = Lists.newArrayList(ObjectType.TABLE,
                    ObjectType.DATABASE,
                    ObjectType.SYSTEM,
                    ObjectType.USER,
                    ObjectType.RESOURCE,
                    ObjectType.VIEW,
                    ObjectType.CATALOG,
                    ObjectType.MATERIALIZED_VIEW,
                    ObjectType.FUNCTION,
                    ObjectType.RESOURCE_GROUP,
                    ObjectType.PIPE,
                    ObjectType.GLOBAL_FUNCTION,
                    ObjectType.STORAGE_VOLUME,
                    ObjectType.WAREHOUSE);
            for (ObjectType objectType : objectTypes) {
                initPrivilegeCollectionAllObjects(rolePrivilegeCollection, objectType,
                        provider.getAvailablePrivType(objectType));
            }
            rolePrivilegeCollection.disableMutable();  // not mutable

            // 2. builtin db_admin role
            rolePrivilegeCollection = initBuiltinRoleUnlocked(PrivilegeBuiltinConstants.DB_ADMIN_ROLE_ID,
                    PrivilegeBuiltinConstants.DB_ADMIN_ROLE_NAME, "built-in database administration role");
            // System grant belong to db_admin
            List<PrivilegeType> dbAdminSystemGrant = Lists.newArrayList(
                    PrivilegeType.CREATE_RESOURCE,
                    PrivilegeType.PLUGIN,
                    PrivilegeType.FILE,
                    PrivilegeType.BLACKLIST,
                    PrivilegeType.OPERATE,
                    PrivilegeType.CREATE_EXTERNAL_CATALOG,
                    PrivilegeType.REPOSITORY,
                    PrivilegeType.CREATE_RESOURCE_GROUP,
                    PrivilegeType.CREATE_GLOBAL_FUNCTION,
                    PrivilegeType.CREATE_STORAGE_VOLUME,
                    PrivilegeType.SECURITY);
            initPrivilegeCollections(rolePrivilegeCollection, ObjectType.SYSTEM, dbAdminSystemGrant, null, false);

            for (ObjectType t : Arrays.asList(
                    ObjectType.CATALOG,
                    ObjectType.DATABASE,
                    ObjectType.TABLE,
                    ObjectType.VIEW,
                    ObjectType.MATERIALIZED_VIEW,
                    ObjectType.RESOURCE,
                    ObjectType.RESOURCE_GROUP,
                    ObjectType.FUNCTION,
                    ObjectType.GLOBAL_FUNCTION,
                    ObjectType.STORAGE_VOLUME,
                    ObjectType.PIPE)) {
                initPrivilegeCollectionAllObjects(rolePrivilegeCollection, t, provider.getAvailablePrivType(t));
            }
            rolePrivilegeCollection.disableMutable(); // not mutable

            // 3. cluster_admin
            rolePrivilegeCollection = initBuiltinRoleUnlocked(PrivilegeBuiltinConstants.CLUSTER_ADMIN_ROLE_ID,
                    PrivilegeBuiltinConstants.CLUSTER_ADMIN_ROLE_NAME, "built-in cluster administration role");
            // GRANT NODE ON SYSTEM
            initPrivilegeCollections(
                    rolePrivilegeCollection,
                    ObjectType.SYSTEM,
                    List.of(PrivilegeType.NODE, PrivilegeType.CREATE_WAREHOUSE),
                    null,
                    false);
            initPrivilegeCollectionAllObjects(rolePrivilegeCollection, ObjectType.WAREHOUSE,
                    provider.getAvailablePrivType(ObjectType.WAREHOUSE));
            rolePrivilegeCollection.disableMutable();

            // 4. user_admin
            rolePrivilegeCollection = initBuiltinRoleUnlocked(PrivilegeBuiltinConstants.USER_ADMIN_ROLE_ID,
                    PrivilegeBuiltinConstants.USER_ADMIN_ROLE_NAME, "built-in user administration role");
            // GRANT GRANT ON SYSTEM
            initPrivilegeCollections(
                    rolePrivilegeCollection,
                    ObjectType.SYSTEM,
                    Collections.singletonList(PrivilegeType.GRANT),
                    null,
                    false);
            initPrivilegeCollectionAllObjects(rolePrivilegeCollection, ObjectType.USER,
                    provider.getAvailablePrivType(ObjectType.USER));
            rolePrivilegeCollection.disableMutable(); // not mutable

            // 5. security_admin
            rolePrivilegeCollection = initBuiltinRoleUnlocked(
                    PrivilegeBuiltinConstants.SECURITY_ADMIN_ROLE_ID,
                    PrivilegeBuiltinConstants.SECURITY_ADMIN_ROLE_NAME,
                    "built-in security administration role");
            // GRANT SECURITY ON SYSTEM
            initPrivilegeCollections(
                    rolePrivilegeCollection,
                    ObjectType.SYSTEM,
                    Arrays.asList(PrivilegeType.SECURITY, PrivilegeType.OPERATE),
                    null,
                    false);
            rolePrivilegeCollection.disableMutable(); // not mutable

            // 6. public
            rolePrivilegeCollection = initBuiltinRoleUnlocked(PrivilegeBuiltinConstants.PUBLIC_ROLE_ID,
                    PrivilegeBuiltinConstants.PUBLIC_ROLE_NAME,
                    "built-in public role which is owned by any user");
            // GRANT SELECT ON ALL TABLES IN information_schema
            List<PEntryObject> object = Collections.singletonList(new TablePEntryObject(
                    Long.toString(SystemId.INFORMATION_SCHEMA_DB_ID), PrivilegeBuiltinConstants.ALL_TABLES_UUID));
            rolePrivilegeCollection.grant(ObjectType.TABLE, Collections.singletonList(PrivilegeType.SELECT), object,
                    false);

            // 7. builtin user root
            UserPrivilegeCollectionV2 rootCollection = new UserPrivilegeCollectionV2();
            rootCollection.grantRole(PrivilegeBuiltinConstants.ROOT_ROLE_ID);
            rootCollection.setDefaultRoleIds(Sets.newHashSet(PrivilegeBuiltinConstants.ROOT_ROLE_ID));
            userToPrivilegeCollection.put(UserIdentity.ROOT, rootCollection);
        } catch (PrivilegeException e) {
            // all initial privileges are supposed to be legal
            throw new RuntimeException("Fatal error when initializing built-in role and user", e);
        }
    }

    // called by initBuiltinRolesAndUsers()
    protected void initPrivilegeCollections(PrivilegeCollectionV2 collection, ObjectType objectType,
                                            List<PrivilegeType> actionList,
                                            List<String> tokens, boolean isGrant) throws PrivilegeException {
        List<PEntryObject> object;
        if (tokens != null) {
            object = Collections.singletonList(provider.generateObject(objectType, tokens));
        } else {
            object = Arrays.asList(new PEntryObject[] {null});
        }
        collection.grant(objectType, actionList, object, isGrant);
    }

    // called by initBuiltinRolesAndUsers()
    protected void initPrivilegeCollectionAllObjects(
            PrivilegeCollectionV2 collection, ObjectType objectType, List<PrivilegeType> actionList)
            throws PrivilegeException {
        List<PEntryObject> objects = new ArrayList<>();
        if (ObjectType.TABLE.equals(objectType)) {
            objects.add(provider.generateObject(objectType,
                    Lists.newArrayList("*", "*", "*")));
            collection.grant(objectType, actionList, objects, false);
        } else if (ObjectType.VIEW.equals(objectType)
                || ObjectType.MATERIALIZED_VIEW.equals(objectType)
                || ObjectType.DATABASE.equals(objectType)
                || ObjectType.PIPE.equals(objectType)) {
            objects.add(provider.generateObject(objectType,
                    Lists.newArrayList("*", "*")));
            collection.grant(objectType, actionList, objects, false);
        } else if (ObjectType.USER.equals(objectType)) {
            objects.add(provider.generateUserObject(objectType, null));
            collection.grant(objectType, actionList, objects, false);
        } else if (ObjectType.RESOURCE.equals(objectType)
                || ObjectType.CATALOG.equals(objectType)
                || ObjectType.RESOURCE_GROUP.equals(objectType)
                || ObjectType.STORAGE_VOLUME.equals(objectType)
                || ObjectType.WAREHOUSE.equals(objectType)) {
            objects.add(provider.generateObject(objectType,
                    Lists.newArrayList("*")));
            collection.grant(objectType, actionList, objects, false);
        } else if (ObjectType.FUNCTION.equals(objectType)) {
            objects.add(provider.generateFunctionObject(objectType,
                    PrivilegeBuiltinConstants.ALL_DATABASE_ID,
                    PrivilegeBuiltinConstants.ALL_FUNCTIONS_ID));
            collection.grant(objectType, actionList, objects, false);
        } else if (ObjectType.GLOBAL_FUNCTION.equals(objectType)) {
            objects.add(provider.generateFunctionObject(objectType,
                    PrivilegeBuiltinConstants.GLOBAL_FUNCTION_DEFAULT_DATABASE_ID,
                    PrivilegeBuiltinConstants.ALL_FUNCTIONS_ID));
            collection.grant(objectType, actionList, objects, false);
        } else if (ObjectType.SYSTEM.equals(objectType)) {
            collection.grant(objectType, actionList, Arrays.asList(new PEntryObject[] {null}), false);
        } else {
            throw new PrivilegeException("unsupported type " + objectType);
        }
    }

    protected RolePrivilegeCollectionV2 initBuiltinRoleUnlocked(long roleId, String name, String comment) {
        RolePrivilegeCollectionV2 collection = new RolePrivilegeCollectionV2(
                name, comment, RolePrivilegeCollectionV2.RoleFlags.MUTABLE);
        roleIdToPrivilegeCollection.put(roleId, collection);
        roleNameToId.put(name, roleId);
        LOG.info("create built-in role {}[{}]", name, roleId);
        return collection;
    }

    private void userReadLock() {
        userLock.readLock().lock();
    }

    private void userReadUnlock() {
        userLock.readLock().unlock();
    }

    private void userWriteLock() {
        userLock.writeLock().lock();
    }

    private void userWriteUnlock() {
        userLock.writeLock().unlock();
    }

    private void roleReadLock() {
        roleLock.readLock().lock();
    }

    private void roleReadUnlock() {
        roleLock.readLock().unlock();
    }

    private void roleWriteLock() {
        roleLock.writeLock().lock();
    }

    private void roleWriteUnlock() {
        roleLock.writeLock().unlock();
    }

    /**
     * Since when modifying role privilege, we need to invalidate privilege cache for users
     * who own this role, and this process needs user read lock, to keep the lock order of
     * AuthenticationMgr lock -> user lock -> role lock and avoid deadlock, we should acquire
     * the user read lock first when doing role modification, like grant to role, drop role etc.
     * The following locking api is created for that purpose.
     */
    public void lockForRoleUpdate() {
        userReadLock();
        roleWriteLock();
    }

    public void unlockForRoleUpdate() {
        roleWriteUnlock();
        userReadUnlock();
    }

    public void grant(GrantPrivilegeStmt stmt) throws DdlException {
        try {
            if (stmt.getRole() != null) {
                grantToRole(
                        stmt.getObjectType(),
                        stmt.getPrivilegeTypes(),
                        stmt.getObjectList(),
                        stmt.isWithGrantOption(),
                        stmt.getRole());
            } else {
                grantToUser(
                        stmt.getObjectType(),
                        stmt.getPrivilegeTypes(),
                        stmt.getObjectList(),
                        stmt.isWithGrantOption(),
                        stmt.getUserIdentity());
            }
        } catch (PrivilegeException e) {
            throw new DdlException("failed to grant: " + e.getMessage(), e);
        }
    }

    protected void grantToUser(
            ObjectType type,
            List<PrivilegeType> privilegeTypes,
            List<PEntryObject> objects,
            boolean isGrant,
            UserIdentity userIdentity) throws PrivilegeException {
        userWriteLock();
        try {
            UserPrivilegeCollectionV2 collection = getUserPrivilegeCollectionUnlocked(userIdentity);
            collection.grant(type, privilegeTypes, objects, isGrant);
            GlobalStateMgr.getCurrentState().getEditLog().logUpdateUserPrivilege(
                    userIdentity, collection, provider.getPluginId(), provider.getPluginVersion());
            invalidateUserInCache(userIdentity);
        } finally {
            userWriteUnlock();
        }
    }

    protected void grantToRole(
            ObjectType objectType,
            List<PrivilegeType> privilegeTypes,
            List<PEntryObject> objects,
            boolean isGrant,
            String roleName) throws PrivilegeException {
        try {
            lockForRoleUpdate();
            long roleId = getRoleIdByNameNoLock(roleName);
            invalidateRolesInCacheRoleUnlocked(roleId);
            RolePrivilegeCollectionV2 collection = getRolePrivilegeCollectionUnlocked(roleId, true);
            collection.grant(objectType, privilegeTypes, objects, isGrant);

            Map<Long, RolePrivilegeCollectionV2> rolePrivCollectionModified = new HashMap<>();
            rolePrivCollectionModified.put(roleId, collection);

            GlobalStateMgr.getCurrentState().getEditLog().logUpdateRolePrivilege(
                    rolePrivCollectionModified, provider.getPluginId(), provider.getPluginVersion());
        } finally {
            unlockForRoleUpdate();
        }
    }

    public void revoke(RevokePrivilegeStmt stmt) throws DdlException {
        try {
            if (stmt.getRole() != null) {
                revokeFromRole(
                        stmt.getObjectType(),
                        stmt.getPrivilegeTypes(),
                        stmt.getObjectList(),
                        stmt.getRole());
            } else {
                revokeFromUser(
                        stmt.getObjectType(),
                        stmt.getPrivilegeTypes(),
                        stmt.getObjectList(),
                        stmt.getUserIdentity());
            }
        } catch (PrivilegeException e) {
            throw new DdlException(e.getMessage());
        }
    }

    protected void revokeFromUser(
            ObjectType objectType,
            List<PrivilegeType> privilegeTypes,
            List<PEntryObject> objects,
            UserIdentity userIdentity) throws PrivilegeException {
        userWriteLock();
        try {
            UserPrivilegeCollectionV2 collection = getUserPrivilegeCollectionUnlocked(userIdentity);
            collection.revoke(objectType, privilegeTypes, objects);
            GlobalStateMgr.getCurrentState().getEditLog().logUpdateUserPrivilege(
                    userIdentity, collection, provider.getPluginId(), provider.getPluginVersion());
            invalidateUserInCache(userIdentity);
        } finally {
            userWriteUnlock();
        }
    }

    protected void revokeFromRole(
            ObjectType objectType,
            List<PrivilegeType> privilegeTypes,
            List<PEntryObject> objects,
            String roleName) throws PrivilegeException {
        try {
            lockForRoleUpdate();
            long roleId = getRoleIdByNameNoLock(roleName);
            RolePrivilegeCollectionV2 collection = getRolePrivilegeCollectionUnlocked(roleId, true);
            collection.revoke(objectType, privilegeTypes, objects);
            invalidateRolesInCacheRoleUnlocked(roleId);

            Map<Long, RolePrivilegeCollectionV2> rolePrivCollectionModified = new HashMap<>();
            rolePrivCollectionModified.put(roleId, collection);

            GlobalStateMgr.getCurrentState().getEditLog().logUpdateRolePrivilege(
                    rolePrivCollectionModified, provider.getPluginId(), provider.getPluginVersion());
        } finally {
            unlockForRoleUpdate();
        }
    }

    public void grantRole(GrantRoleStmt stmt) throws DdlException {
        try {
            if (stmt.getUserIdentity() != null) {
                grantRoleToUser(stmt.getGranteeRole(), stmt.getUserIdentity());
            } else {
                grantRoleToRole(stmt.getGranteeRole(), stmt.getRole());
            }
        } catch (PrivilegeException e) {
            throw new DdlException("failed to grant role: " + e.getMessage(), e);
        }
    }

    protected void grantRoleToUser(List<String> parentRoleName, UserIdentity user) throws PrivilegeException {
        userWriteLock();
        try {
            UserPrivilegeCollectionV2 userPrivilegeCollection = getUserPrivilegeCollectionUnlocked(user);

            roleReadLock();
            try {
                for (String parentRole : parentRoleName) {
                    long roleId = getRoleIdByNameNoLock(parentRole);

                    // public cannot be revoked!
                    if (roleId == PrivilegeBuiltinConstants.PUBLIC_ROLE_ID) {
                        throw new PrivilegeException("Granting role PUBLIC has no effect.  " +
                                "Every user and role has role PUBLIC implicitly granted.");
                    }

                    // temporarily add parent role to user to verify predecessors
                    userPrivilegeCollection.grantRole(roleId);
                    boolean verifyDone = false;
                    try {
                        Set<Long> result = getAllPredecessorRoleIdsUnlocked(userPrivilegeCollection);
                        if (result.size() > Config.privilege_max_total_roles_per_user) {
                            LOG.warn("too many predecessor roles {} for user {}", result, user);
                            throw new PrivilegeException(String.format(
                                    "%s has total %d predecessor roles > %d!",
                                    user, result.size(), Config.privilege_max_total_roles_per_user));
                        }
                        verifyDone = true;
                    } finally {
                        if (!verifyDone) {
                            userPrivilegeCollection.revokeRole(roleId);
                        }
                    }
                }
            } finally {
                roleReadUnlock();
            }

            GlobalStateMgr.getCurrentState().getEditLog().logUpdateUserPrivilege(
                    user, userPrivilegeCollection, provider.getPluginId(), provider.getPluginVersion());
            invalidateUserInCache(user);
            LOG.info("grant role {} to user {}", Joiner.on(", ").join(parentRoleName), user);
        } finally {
            userWriteUnlock();
        }
    }

    protected void grantRoleToRole(List<String> parentRoleName, String roleName) throws PrivilegeException {
        try {
            lockForRoleUpdate();
            long roleId = getRoleIdByNameNoLock(roleName);
            RolePrivilegeCollectionV2 collection = getRolePrivilegeCollectionUnlocked(roleId, true);

            Map<Long, RolePrivilegeCollectionV2> rolePrivCollectionModified = new HashMap<>();
            rolePrivCollectionModified.put(roleId, collection);
            for (String parentRole : parentRoleName) {
                long parentRoleId = getRoleIdByNameNoLock(parentRole);

                if (parentRoleId == PrivilegeBuiltinConstants.PUBLIC_ROLE_ID) {
                    throw new PrivilegeException("Granting role PUBLIC has no effect.  " +
                            "Every user and role has role PUBLIC implicitly granted.");
                }

                RolePrivilegeCollectionV2 parentCollection =
                        getRolePrivilegeCollectionUnlocked(parentRoleId, true);

                // to avoid circle, verify roleName is not predecessor role of parentRoleName
                Set<Long> parentRolePredecessors = getAllPredecessorRoleIdsUnlocked(parentRoleId);
                if (parentRolePredecessors.contains(roleId)) {
                    throw new PrivilegeException(String.format("role %s[%d] is already a predecessor role of %s[%d]",
                            roleName, roleId, parentRole, parentRoleId));
                }

                // temporarily add sub role to parent role to verify inheritance depth
                boolean verifyDone = false;
                parentCollection.addSubRole(roleId);
                try {
                    // verify role inheritance depth
                    parentRolePredecessors = getAllPredecessorRoleIdsUnlocked(parentRoleId);
                    parentRolePredecessors.add(parentRoleId);
                    for (long i : parentRolePredecessors) {
                        long cnt = getMaxRoleInheritanceDepthInner(0, i);
                        if (cnt > Config.privilege_max_role_depth) {
                            String name = getRolePrivilegeCollectionUnlocked(i, true).getName();
                            throw new PrivilegeException(String.format(
                                    "role inheritance depth for %s[%d] is %d > %d",
                                    name, i, cnt, Config.privilege_max_role_depth));
                        }
                    }

                    verifyDone = true;
                } finally {
                    if (!verifyDone) {
                        parentCollection.removeSubRole(roleId);
                    }
                }

                collection.addParentRole(parentRoleId);

                if (PrivilegeBuiltinConstants.IMMUTABLE_BUILT_IN_ROLE_IDS.contains(parentRoleId)) {
                    RolePrivilegeCollectionV2 clone = parentCollection.cloneSelf();
                    clone.typeToPrivilegeEntryList = new HashMap<>();
                    rolePrivCollectionModified.put(parentRoleId, clone);
                } else {
                    rolePrivCollectionModified.put(parentRoleId, parentCollection);
                }
            }

            invalidateRolesInCacheRoleUnlocked(roleId);

            // write journal to update privilege collections of both role & parent role
            RolePrivilegeCollectionInfo info = new RolePrivilegeCollectionInfo(
                    rolePrivCollectionModified, provider.getPluginId(), provider.getPluginVersion());
            GlobalStateMgr.getCurrentState().getEditLog().logUpdateRolePrivilege(info);
            LOG.info("grant role {}[{}] to role {}[{}]", parentRoleName,
                    Joiner.on(", ").join(parentRoleName), roleName, roleId);
        } finally {
            unlockForRoleUpdate();
        }
    }

    public void revokeRole(RevokeRoleStmt stmt) throws DdlException {
        try {
            if (stmt.getUserIdentity() != null) {
                revokeRoleFromUser(stmt.getGranteeRole(), stmt.getUserIdentity());
            } else {
                revokeRoleFromRole(stmt.getGranteeRole(), stmt.getRole());
            }
        } catch (PrivilegeException e) {
            throw new DdlException("failed to revoke role: " + e.getMessage(), e);
        }
    }

    protected void revokeRoleFromUser(List<String> roleNameList, UserIdentity user) throws PrivilegeException {
        userWriteLock();
        try {
            UserPrivilegeCollectionV2 collection = getUserPrivilegeCollectionUnlocked(user);
            roleReadLock();
            try {
                for (String roleName : roleNameList) {
                    long roleId = getRoleIdByNameNoLock(roleName);
                    // public cannot be revoked!
                    if (roleId == PrivilegeBuiltinConstants.PUBLIC_ROLE_ID) {
                        throw new PrivilegeException("Revoking role PUBLIC has no effect.  " +
                                "Every user and role has role PUBLIC implicitly granted.");
                    }
                    collection.revokeRole(roleId);
                }
            } finally {
                roleReadUnlock();
            }
            GlobalStateMgr.getCurrentState().getEditLog().logUpdateUserPrivilege(
                    user, collection, provider.getPluginId(), provider.getPluginVersion());
            invalidateUserInCache(user);
            LOG.info("revoke role {} from user {}", roleNameList.toString(), user);
        } finally {
            userWriteUnlock();
        }
    }

    protected void revokeRoleFromRole(List<String> parentRoleNameList, String roleName) throws PrivilegeException {
        try {
            lockForRoleUpdate();
            long roleId = getRoleIdByNameNoLock(roleName);
            RolePrivilegeCollectionV2 collection = getRolePrivilegeCollectionUnlocked(roleId, true);

            for (String parentRoleName : parentRoleNameList) {
                long parentRoleId = getRoleIdByNameNoLock(parentRoleName);

                if (parentRoleId == PrivilegeBuiltinConstants.PUBLIC_ROLE_ID) {
                    throw new PrivilegeException("Revoking role PUBLIC has no effect.  " +
                            "Every user and role has role PUBLIC implicitly granted.");
                }

                RolePrivilegeCollectionV2 parentCollection =
                        getRolePrivilegeCollectionUnlocked(parentRoleId, true);
                parentCollection.removeSubRole(roleId);
                collection.removeParentRole(parentRoleId);
            }

            Map<Long, RolePrivilegeCollectionV2> rolePrivCollectionModified = new HashMap<>();
            rolePrivCollectionModified.put(roleId, collection);

            List<Long> parentRoleIdList = new ArrayList<>();
            for (String parentRoleName : parentRoleNameList) {
                long parentRoleId = getRoleIdByNameNoLock(parentRoleName);
                RolePrivilegeCollectionV2 parentCollection =
                        getRolePrivilegeCollectionUnlocked(parentRoleId, true);
                parentRoleIdList.add(parentRoleId);
                if (PrivilegeBuiltinConstants.IMMUTABLE_BUILT_IN_ROLE_IDS.contains(parentRoleId)) {
                    RolePrivilegeCollectionV2 clone = parentCollection.cloneSelf();
                    clone.typeToPrivilegeEntryList = new HashMap<>();
                    rolePrivCollectionModified.put(parentRoleId, clone);
                } else {
                    rolePrivCollectionModified.put(parentRoleId, parentCollection);
                }
            }

            // write journal to update privilege collections of both role & parent role
            RolePrivilegeCollectionInfo info = new RolePrivilegeCollectionInfo(
                    rolePrivCollectionModified, provider.getPluginId(), provider.getPluginVersion());
            GlobalStateMgr.getCurrentState().getEditLog().logUpdateRolePrivilege(info);
            invalidateRolesInCacheRoleUnlocked(roleId);
            LOG.info("revoke role {}[{}] from role {}[{}]",
                    parentRoleNameList.toString(), parentRoleIdList.toString(), roleName, roleId);
        } finally {
            unlockForRoleUpdate();
        }
    }

    public void validateGrant(ObjectType objectType, List<PrivilegeType> privilegeTypes, List<PEntryObject> objects)
            throws PrivilegeException {
        provider.validateGrant(objectType, privilegeTypes, objects);
    }

    public static Set<Long> getOwnedRolesByUser(UserIdentity userIdentity) throws PrivilegeException {
        AuthorizationMgr manager = GlobalStateMgr.getCurrentState().getAuthorizationMgr();
        try {
            manager.userReadLock();
            UserPrivilegeCollectionV2 userCollection = manager.getUserPrivilegeCollectionUnlocked(userIdentity);
            return userCollection.getAllRoles();
        } finally {
            manager.userReadUnlock();
        }
    }

    protected boolean checkAction(PrivilegeCollectionV2 collection, ObjectType objectType,
                                  PrivilegeType privilegeType, List<String> objectNames) throws PrivilegeException {
        if (objectNames == null) {
            return provider.check(objectType, privilegeType, null, collection);
        } else {
            PEntryObject object = provider.generateObject(objectType, objectNames);
            return provider.check(objectType, privilegeType, object, collection);
        }
    }

    public boolean canExecuteAs(ConnectContext context, UserIdentity impersonateUser) {
        try {
            PrivilegeCollectionV2 collection = mergePrivilegeCollection(
                    context.getCurrentUserIdentity(), context.getGroups(), context.getCurrentRoleIds());
            PEntryObject object = provider.generateUserObject(ObjectType.USER, impersonateUser);
            return provider.check(ObjectType.USER, PrivilegeType.IMPERSONATE, object, collection);
        } catch (PrivilegeException e) {
            LOG.warn("caught exception in canExecuteAs() user[{}]", impersonateUser, e);
            return false;
        }
    }

    public boolean allowGrant(UserIdentity currentUser, Set<String> groups, Set<Long> roleIds, ObjectType type,
                              List<PrivilegeType> wants, List<PEntryObject> objects) {
        try {
            PrivilegeCollectionV2 collection = mergePrivilegeCollection(currentUser, groups, roleIds);
            // check for WITH GRANT OPTION in the specific type
            return provider.allowGrant(type, wants, objects, collection);
        } catch (PrivilegeException e) {
            LOG.warn("caught exception when allowGrant", e);
            return false;
        }
    }

    public void replayUpdateUserPrivilegeCollection(
            UserIdentity user,
            UserPrivilegeCollectionV2 privilegeCollection,
            short pluginId,
            short pluginVersion) throws PrivilegeException {
        userWriteLock();
        try {
            userToPrivilegeCollection.put(user, privilegeCollection);
            invalidateUserInCache(user);
            LOG.info("replayed update user {}", user);
        } finally {
            userWriteUnlock();
        }
    }

    /**
     * init all builtin privilege when a user is created, called by AuthenticationManager
     */
    public UserPrivilegeCollectionV2 onCreateUser(UserIdentity user,
                                                  List<String> defaultRoleName) throws PrivilegeException {
        userWriteLock();
        try {
            UserPrivilegeCollectionV2 privilegeCollection = new UserPrivilegeCollectionV2();

            if (!defaultRoleName.isEmpty()) {
                Set<Long> roleIds = new HashSet<>();
                for (String role : defaultRoleName) {
                    Long roleId = getRoleIdByNameNoLock(role);
                    privilegeCollection.grantRole(roleId);
                    roleIds.add(roleId);
                }
                privilegeCollection.setDefaultRoleIds(roleIds);
            }

            userToPrivilegeCollection.put(user, privilegeCollection);
            LOG.info("user privilege for {} is created, role {} is granted",
                    user, PrivilegeBuiltinConstants.PUBLIC_ROLE_NAME);
            return privilegeCollection;
        } finally {
            userWriteUnlock();
        }
    }

    /**
     * drop user privilege collection when a user is dropped, called by AuthenticationManager
     */
    public void onDropUser(UserIdentity user) {
        userWriteLock();
        try {
            userToPrivilegeCollection.remove(user);
            invalidateUserInCache(user);
        } finally {
            userWriteUnlock();
        }
    }

    public short getProviderPluginId() {
        return provider.getPluginId();
    }

    public short getProviderPluginVersion() {
        return provider.getPluginVersion();
    }

    /**
     * read from cache
     */
    protected PrivilegeCollectionV2 mergePrivilegeCollection(UserIdentity userIdentity, Set<String> groups, Set<Long> roleIds)
            throws PrivilegeException {
        try {
            if (Config.authorization_enable_priv_collection_cache) {
                return ctxToMergedPrivilegeCollections.get(new UserPrivKey(userIdentity, groups, roleIds));
            } else {
                return loadPrivilegeCollection(userIdentity, groups, roleIds);
            }
        } catch (ExecutionException e) {
            String errMsg = String.format("failed merge privilege collection on %s with roles %s", userIdentity, roleIds);
            PrivilegeException exception = new PrivilegeException(errMsg);
            exception.initCause(e);
            throw exception;
        }
    }

    /**
     * used for cache to do the actual merge job
     */
    protected PrivilegeCollectionV2 loadPrivilegeCollection(UserIdentity userIdentity, Set<String> groups,
                                                            Set<Long> roleIdsSpecified)
            throws PrivilegeException {
        PrivilegeCollectionV2 collection = new PrivilegeCollectionV2();
        try {
            userReadLock();
            Set<Long> validRoleIds;
            if (userIdentity.isEphemeral()) {
                Preconditions.checkState(roleIdsSpecified != null,
                        "ephemeral use should always have current role ids specified");
                validRoleIds = roleIdsSpecified;
            } else {
                // Merge privileges directly granted to user first.
                UserPrivilegeCollectionV2 userPrivilegeCollection = getUserPrivilegeCollectionUnlocked(userIdentity);
                collection.merge(userPrivilegeCollection);
                // 1. Get all parent roles by default, but we support user to activate only part
                // of the owned roles by `SET ROLE` statement, in this case we restrict
                // the privileges from roles to only the activated ones.
                validRoleIds = new HashSet<>(userPrivilegeCollection.getAllRoles());
                if (roleIdsSpecified != null) {
                    validRoleIds.retainAll(roleIdsSpecified);
                }
            }

            try {
                roleReadLock();
                // 2. Get all predecessors base on step 1
                // The main purpose of the secondary verification of UserPrivilegeCollection here is.
                // Because the user's permissions may be revoked while the session is not disconnected,
                // the role list stored in the session cannot be changed at this time
                // (because the current session and the session initiated by the revoke operation may not be the same),
                // but for the user The operation will cause the cache to invalid, so in the next load process after
                // the cache fails, we need to determine whether the user still has access to this role.
                validRoleIds = getAllPredecessorRoleIdsUnlocked(validRoleIds);

                // 3. Merge privilege collections of all predecessors.
                for (long roleId : validRoleIds) {
                    // Because the drop role is an asynchronous behavior, the parentRole may not exist.
                    // Here, for the role that does not exist, choose to ignore it directly
                    RolePrivilegeCollectionV2 rolePrivilegeCollection =
                            getRolePrivilegeCollectionUnlocked(roleId, false);
                    if (rolePrivilegeCollection != null) {
                        collection.merge(rolePrivilegeCollection);
                    }
                }

                RolePrivilegeCollectionV2 rolePrivilegeCollection =
                        getRolePrivilegeCollectionUnlocked(PrivilegeBuiltinConstants.PUBLIC_ROLE_ID, false);
                if (rolePrivilegeCollection != null) {
                    collection.merge(rolePrivilegeCollection);
                }

            } finally {
                roleReadUnlock();
            }
        } finally {
            userReadUnlock();
        }
        return collection;
    }

    /**
     * if the privileges of a role are changed, call this function to invalidate cache
     * requires role lock
     */
    protected void invalidateRolesInCacheRoleUnlocked(long roleId) throws PrivilegeException {
        Set<Long> badRoles = getAllDescendantsUnlocked(roleId);
        List<UserPrivKey> badKeys = new ArrayList<>();
        for (UserPrivKey pair : ctxToMergedPrivilegeCollections.asMap().keySet()) {
            Set<Long> roleIds = pair.roleIds;
            if (roleIds == null) {
                roleIds = getRoleIdsByUser(pair.userIdentity);
            }

            for (long badRoleId : badRoles) {
                if (roleIds.contains(badRoleId)) {
                    badKeys.add(pair);
                    break;
                }
            }
        }
        for (UserPrivKey pair : badKeys) {
            ctxToMergedPrivilegeCollections.invalidate(pair);
        }
    }

    /**
     * if the privileges of a user are changed, call this function to invalidate cache
     * require not extra lock.
     */
    protected void invalidateUserInCache(UserIdentity userIdentity) {
        List<UserPrivKey> badKeys = new ArrayList<>();
        for (UserPrivKey pair : ctxToMergedPrivilegeCollections.asMap().keySet()) {
            if (pair.userIdentity.equals(userIdentity)) {
                badKeys.add(pair);
            }
        }
        for (UserPrivKey pair : badKeys) {
            ctxToMergedPrivilegeCollections.invalidate(pair);
        }
    }

    public UserPrivilegeCollectionV2 getUserPrivilegeCollection(UserIdentity userIdentity) {
        userReadLock();
        try {
            return userToPrivilegeCollection.get(userIdentity);
        } finally {
            userReadUnlock();
        }
    }

    public UserPrivilegeCollectionV2 getUserPrivilegeCollectionUnlocked(UserIdentity userIdentity)
            throws PrivilegeException {
        UserPrivilegeCollectionV2 userCollection = userToPrivilegeCollection.get(userIdentity);
        if (userCollection == null) {
            throw new PrivilegeException("cannot find user " + (userIdentity == null ? "null" :
                    userIdentity.toString()));
        }
        return userCollection;
    }

    public List<String> getAllUsers() {
        userReadLock();
        try {
            List<String> users = Lists.newArrayList();
            Set<UserIdentity> userIdentities = userToPrivilegeCollection.keySet();
            for (UserIdentity userIdentity : userIdentities) {
                users.add(userIdentity.toString());
            }
            return users;
        } finally {
            userReadUnlock();
        }
    }

    public Set<UserIdentity> getAllUserIdentities() {
        userReadLock();
        try {
            return userToPrivilegeCollection.keySet();
        } finally {
            userReadUnlock();
        }
    }

    // return null if not exists
    protected UserPrivilegeCollectionV2 getUserPrivilegeCollectionUnlockedAllowNull(UserIdentity userIdentity) {
        return userToPrivilegeCollection.get(userIdentity);
    }

    public RolePrivilegeCollectionV2 getRolePrivilegeCollection(String roleName) {
        roleReadLock();
        try {
            Long roleId = roleNameToId.get(roleName);
            if (roleId == null) {
                return null;
            }
            return roleIdToPrivilegeCollection.get(roleId);
        } finally {
            roleReadUnlock();
        }
    }

    public RolePrivilegeCollectionV2 getRolePrivilegeCollection(long roleId) {
        roleReadLock();
        try {
            return roleIdToPrivilegeCollection.get(roleId);
        } finally {
            roleReadUnlock();
        }
    }

    public void getRecursiveRole(Set<String> roleNames, Long roleId) {
        RolePrivilegeCollectionV2 rolePrivilegeCollection = getRolePrivilegeCollection(roleId);
        if (rolePrivilegeCollection != null) {
            roleNames.add(rolePrivilegeCollection.getName());

            for (Long parentId : rolePrivilegeCollection.getParentRoleIds()) {
                getRecursiveRole(roleNames, parentId);
            }
        }
    }

    public RolePrivilegeCollectionV2 getRolePrivilegeCollectionUnlocked(long roleId, boolean exceptionIfNotExists)
            throws PrivilegeException {
        RolePrivilegeCollectionV2 collection = roleIdToPrivilegeCollection.get(roleId);
        if (collection == null) {
            if (exceptionIfNotExists) {
                throw new PrivilegeException("cannot find role" + roleId);
            } else {
                return null;
            }
        }
        return collection;
    }

    public List<String> getGranteeRoleDetailsForRole(String roleName) {
        roleReadLock();
        try {
            Long roleId = getRoleIdByNameAllowNull(roleName);
            if (roleId == null) {
                throw new SemanticException("cannot find role " + roleName);
            }

            RolePrivilegeCollectionV2 rolePrivilegeCollection =
                    getRolePrivilegeCollectionUnlocked(roleId, true);

            List<String> parentRoleNameList = new ArrayList<>();
            for (Long parentRoleId : rolePrivilegeCollection.getParentRoleIds()) {
                // Because the drop role is an asynchronous behavior, the parentRole may not exist.
                // Here, for the role that does not exist, choose to ignore it directly
                RolePrivilegeCollectionV2 parentRolePriv =
                        getRolePrivilegeCollectionUnlocked(parentRoleId, false);
                if (parentRolePriv != null) {
                    parentRoleNameList.add(parentRolePriv.getName());
                }
            }

            if (!parentRoleNameList.isEmpty()) {
                return Lists.newArrayList(roleName, null,
                        AstToSQLBuilder.toSQL(new GrantRoleStmt(parentRoleNameList, roleName)));
            }
            return null;
        } catch (PrivilegeException e) {
            throw new SemanticException(e.getMessage());
        } finally {
            roleReadUnlock();
        }
    }

    public Map<ObjectType, List<PrivilegeEntry>> getTypeToPrivilegeEntryListByRole(String roleName) {
        roleReadLock();
        try {
            Long roleId = getRoleIdByNameAllowNull(roleName);
            if (roleId == null) {
                throw new SemanticException("cannot find role " + roleName);
            }

            RolePrivilegeCollectionV2 rolePrivilegeCollection =
                    getRolePrivilegeCollectionUnlocked(roleId, true);
            return rolePrivilegeCollection.getTypeToPrivilegeEntryList();
        } catch (PrivilegeException e) {
            throw new SemanticException(e.getMessage());
        } finally {
            roleReadUnlock();
        }
    }

    public List<String> getGranteeRoleDetailsForUser(UserIdentity userIdentity) {
        userReadLock();
        try {
            Set<Long> allRoles = getRoleIdsByUserUnlocked(userIdentity);

            roleReadLock();
            try {
                List<String> parentRoleNameList = new ArrayList<>();
                for (Long roleId : allRoles) {
                    // Because the drop role is an asynchronous behavior, the parentRole may not exist.
                    // Here, for the role that does not exist, choose to ignore it directly
                    RolePrivilegeCollectionV2 parentRolePriv =
                            getRolePrivilegeCollectionUnlocked(roleId, false);
                    if (parentRolePriv != null) {
                        parentRoleNameList.add(parentRolePriv.getName());
                    }
                }

                if (!parentRoleNameList.isEmpty()) {
                    return Lists.newArrayList(userIdentity.toString(), null,
                            AstToSQLBuilder.toSQL(new GrantRoleStmt(parentRoleNameList, userIdentity)));
                }
                return null;
            } finally {
                roleReadUnlock();
            }
        } catch (PrivilegeException e) {
            throw new SemanticException(e.getMessage());
        } finally {
            userReadUnlock();
        }
    }

    public Map<ObjectType, List<PrivilegeEntry>> getTypeToPrivilegeEntryListByUser(
            UserIdentity userIdentity) {
        userReadLock();
        try {
            UserPrivilegeCollectionV2 userPrivilegeCollection = getUserPrivilegeCollectionUnlocked(userIdentity);
            return userPrivilegeCollection.getTypeToPrivilegeEntryList();
        } catch (PrivilegeException e) {
            throw new SemanticException(e.getMessage());
        } finally {
            userReadUnlock();
        }
    }

    public List<String> getAllRoles() {
        roleReadLock();
        try {
            List<String> roles = new ArrayList<>();
            for (RolePrivilegeCollectionV2 rolePrivilegeCollection : roleIdToPrivilegeCollection.values()) {
                roles.add(rolePrivilegeCollection.getName());
            }
            return roles;
        } finally {
            roleReadUnlock();
        }
    }

    public List<PrivilegeType> analyzeActionSet(ObjectType objectType, ActionSet actionSet) {
        List<PrivilegeType> privilegeTypes = provider.getAvailablePrivType(objectType);
        List<PrivilegeType> actions = new ArrayList<>();
        for (PrivilegeType actionName : privilegeTypes) {
            if (actionSet.contains(actionName)) {
                actions.add(actionName);
            }
        }
        return actions;
    }

    public boolean isAvailablePrivType(ObjectType objectType, PrivilegeType privilegeType) {
        return provider.isAvailablePrivType(objectType, privilegeType);
    }

    public PrivilegeType getPrivilegeType(String privTypeString) {
        return provider.getPrivilegeType(privTypeString);
    }

    public ObjectType getObjectType(String objectTypeUnResolved) {
        return provider.getObjectType(objectTypeUnResolved);
    }

    public List<PrivilegeType> getAvailablePrivType(ObjectType objectType) {
        return provider.getAvailablePrivType(objectType);
    }

    public void createRole(CreateRoleStmt stmt) {
        roleWriteLock();
        try {
            Map<String, Long> roleNameToBeCreated = new HashMap<>();
            Map<Long, RolePrivilegeCollectionV2> rolePrivCollectionModified = new HashMap<>();
            for (String roleName : stmt.getRoles()) {
                if (roleNameToId.containsKey(roleName)) {
                    // Existence verification has been performed in the Analyzer stage. If it exists here,
                    // it may be that other threads have performed the same operation, and return directly here
                    LOG.info("Operation CREATE ROLE failed for " + roleName + " : role " + roleName + " already exists");
                    return;
                }

                long roleId = GlobalStateMgr.getCurrentState().getNextId();
                RolePrivilegeCollectionV2 collection = new RolePrivilegeCollectionV2(
                        roleName, stmt.getComment(),
                        RolePrivilegeCollectionV2.RoleFlags.REMOVABLE, RolePrivilegeCollectionV2.RoleFlags.MUTABLE);
                rolePrivCollectionModified.put(roleId, collection);

                roleNameToBeCreated.put(roleName, roleId);
            }

            roleIdToPrivilegeCollection.putAll(rolePrivCollectionModified);
            roleNameToId.putAll(roleNameToBeCreated);

            GlobalStateMgr.getCurrentState().getEditLog().logUpdateRolePrivilege(
                    rolePrivCollectionModified, provider.getPluginId(), provider.getPluginVersion());
            LOG.info("created role {}[{}]", stmt.getRoles().toString(), roleNameToBeCreated.values());
        } finally {
            roleWriteUnlock();
        }
    }

    public void alterRole(AlterRoleStmt stmt) throws DdlException {
        try {
            roleWriteLock();
            // The RolePrivilegeCollections to be modified, for atomicity reason, we do the modification
            // after all checks passed.
            Map<Long, RolePrivilegeCollectionV2> rolePrivCollectionModified = new HashMap<>();
            for (String roleName : stmt.getRoles()) {
                if (!roleNameToId.containsKey(roleName)) {
                    throw new DdlException(roleName + " doesn't exist");
                }
                long roleId = roleNameToId.get(roleName);
                RolePrivilegeCollectionV2 rolePrivilegeCollection =
                        roleIdToPrivilegeCollection.get(roleId);
                Preconditions.checkNotNull(rolePrivilegeCollection);
                if (!rolePrivilegeCollection.isMutable()) {
                    throw new DdlException(roleName + " is immutable");
                }
                rolePrivCollectionModified.put(roleId, rolePrivilegeCollection);
            }

            // apply the modification
            rolePrivCollectionModified.values().forEach(
                    rolePrivilegeCollection -> rolePrivilegeCollection.setComment(stmt.getComment()));

            GlobalStateMgr.getCurrentState().getEditLog().logUpdateRolePrivilege(
                    rolePrivCollectionModified, provider.getPluginId(), provider.getPluginVersion());
        } finally {
            roleWriteUnlock();
        }
    }

    public void replayUpdateRolePrivilegeCollection(
            RolePrivilegeCollectionInfo info) throws PrivilegeException {
        roleWriteLock();
        try {
            for (Map.Entry<Long, RolePrivilegeCollectionV2> entry : info.getRolePrivCollectionModified().entrySet()) {
                long roleId = entry.getKey();
                invalidateRolesInCacheRoleUnlocked(roleId);
                RolePrivilegeCollectionV2 privilegeCollection = entry.getValue();

                if (PrivilegeBuiltinConstants.IMMUTABLE_BUILT_IN_ROLE_IDS.contains(roleId)) {
                    RolePrivilegeCollectionV2 builtInRolePrivilegeCollection = this.roleIdToPrivilegeCollection.get(roleId);
                    privilegeCollection.typeToPrivilegeEntryList = builtInRolePrivilegeCollection.typeToPrivilegeEntryList;
                }
                roleIdToPrivilegeCollection.put(roleId, privilegeCollection);

                if (!roleNameToId.containsKey(privilegeCollection.getName())) {
                    roleNameToId.put(privilegeCollection.getName(), roleId);
                }
                LOG.info("replayed update role {}", roleId);
            }
        } finally {
            roleWriteUnlock();
        }
    }

    public void dropRole(DropRoleStmt stmt) throws DdlException {
        try {
            lockForRoleUpdate();
            List<String> roleNameToBeDropped = new ArrayList<>();
            Map<Long, RolePrivilegeCollectionV2> rolePrivCollectionModified = new HashMap<>();
            for (String roleName : stmt.getRoles()) {
                if (!roleNameToId.containsKey(roleName)) {
                    // Existence verification has been performed in the Analyzer stage. If it not exists here,
                    // it may be that other threads have performed the same operation, and return directly here
                    LOG.info("Operation DROP ROLE failed for " + roleName + " : role " + roleName + " not exists");

                    return;
                }

                long roleId = getRoleIdByNameNoLock(roleName);
                RolePrivilegeCollectionV2 collection = roleIdToPrivilegeCollection.get(roleId);
                if (!collection.isRemovable()) {
                    throw new DdlException("role " + roleName + " cannot be dropped!");
                }

                roleNameToBeDropped.add(roleName);
                rolePrivCollectionModified.put(roleId, collection);
                invalidateRolesInCacheRoleUnlocked(roleId);
            }

            roleIdToPrivilegeCollection.keySet().removeAll(rolePrivCollectionModified.keySet());
            roleNameToBeDropped.forEach(roleNameToId.keySet()::remove);

            GlobalStateMgr.getCurrentState().getEditLog().logDropRole(
                    rolePrivCollectionModified, provider.getPluginId(), provider.getPluginVersion());
            LOG.info("dropped role {}[{}]",
                    stmt.getRoles().toString(), rolePrivCollectionModified.keySet().toString());
        } catch (PrivilegeException e) {
            throw new DdlException("failed to drop role: " + e.getMessage(), e);
        } finally {
            unlockForRoleUpdate();
        }
    }

    public void replayDropRole(
            RolePrivilegeCollectionInfo info) throws PrivilegeException {
        roleWriteLock();
        try {
            for (Map.Entry<Long, RolePrivilegeCollectionV2> entry : info.getRolePrivCollectionModified().entrySet()) {
                long roleId = entry.getKey();
                invalidateRolesInCacheRoleUnlocked(roleId);
                RolePrivilegeCollectionV2 privilegeCollection = entry.getValue();
                roleIdToPrivilegeCollection.remove(roleId);
                roleNameToId.remove(privilegeCollection.getName());
                LOG.info("replayed drop role {}", roleId);
            }
        } finally {
            roleWriteUnlock();
        }
    }

    public boolean checkRoleExists(String name) {
        roleReadLock();
        try {
            return roleNameToId.containsKey(name);
        } finally {
            roleReadUnlock();
        }
    }

    public boolean isBuiltinRole(String name) {
        return PrivilegeBuiltinConstants.BUILT_IN_ROLE_NAMES.contains(name);
    }

    public String getRoleComment(String name) {
        try {
            roleReadLock();
            String result = FeConstants.NULL_STRING;
            Long roleId = roleNameToId.get(name);
            if (roleId != null) {
                String comment = roleIdToPrivilegeCollection.get(roleId).getComment();
                if (!Strings.isNullOrEmpty(comment)) {
                    result = comment;
                }
            }
            return result;
        } finally {
            roleReadUnlock();
        }
    }

    protected Set<Long> getRoleIdsByUserUnlocked(UserIdentity user) throws PrivilegeException {
        Set<Long> ret = new HashSet<>();

        for (long roleId : getUserPrivilegeCollectionUnlocked(user).getAllRoles()) {
            // role may be removed
            if (getRolePrivilegeCollectionUnlocked(roleId, false) != null) {
                ret.add(roleId);
            }
        }
        return ret;
    }

    // used in executing `set role` statement
    public Set<Long> getRoleIdsByUser(UserIdentity user) throws PrivilegeException {
        userReadLock();
        try {
            roleReadLock();
            try {
                return getRoleIdsByUserUnlocked(user);
            } finally {
                roleReadUnlock();
            }
        } finally {
            userReadUnlock();
        }
    }

    public Set<Long> getDefaultRoleIdsByUser(UserIdentity user) throws PrivilegeException {
        userReadLock();
        try {
            Set<Long> ret = new HashSet<>();
            roleReadLock();
            try {
                for (long roleId : getUserPrivilegeCollectionUnlocked(user).getDefaultRoleIds()) {
                    // role may be removed
                    if (getRolePrivilegeCollectionUnlocked(roleId, false) != null) {
                        ret.add(roleId);
                    }
                }
                return ret;
            } finally {
                roleReadUnlock();
            }
        } finally {
            userReadUnlock();
        }
    }

    public void setUserDefaultRole(Set<Long> roleName, UserIdentity user) throws PrivilegeException {
        userWriteLock();
        try {
            UserPrivilegeCollectionV2 collection = getUserPrivilegeCollectionUnlocked(user);

            roleReadLock();
            try {
                collection.setDefaultRoleIds(roleName);
            } finally {
                roleReadUnlock();
            }

            GlobalStateMgr.getCurrentState().getEditLog().logUpdateUserPrivilege(
                    user, collection, provider.getPluginId(), provider.getPluginVersion());
            LOG.info("grant role {} to user {}", roleName, user);
        } finally {
            userWriteUnlock();
        }
    }

    public List<String> getRoleNamesByUser(UserIdentity user) throws PrivilegeException {
        try {
            userReadLock();
            List<String> roleNameList = Lists.newArrayList();
            try {
                roleReadLock();
                for (long roleId : getUserPrivilegeCollectionUnlocked(user).getAllRoles()) {
                    RolePrivilegeCollectionV2 rolePrivilegeCollection =
                            getRolePrivilegeCollectionUnlocked(roleId, false);
                    // role may be removed
                    if (rolePrivilegeCollection != null) {
                        roleNameList.add(rolePrivilegeCollection.getName());
                    }
                }
                return roleNameList;
            } finally {
                roleReadUnlock();
            }
        } finally {
            userReadUnlock();
        }
    }

    public List<String> getRoleNamesByRoleIds(Set<Long> roleIds) {
        List<String> roleNames = new ArrayList<>();
        if (roleIds != null) {
            for (Long roleId : roleIds) {
                RolePrivilegeCollectionV2 roleCollection = getRolePrivilegeCollection(roleId);
                if (roleCollection != null) {
                    roleNames.add(roleCollection.getName());
                }
            }
        }

        return roleNames;
    }

    public List<String> getInactivatedRoleNamesByUser(UserIdentity userIdentity, List<String> activatedRoleNames) {
        List<String> inactivatedRoleNames = new ArrayList<>();
        try {
            inactivatedRoleNames = getRoleNamesByUser(userIdentity);
        } catch (PrivilegeException e) {
            // ignore exception
        }
        inactivatedRoleNames.removeAll(activatedRoleNames);

        return inactivatedRoleNames;
    }

    // used in executing `set role` statement
    public Long getRoleIdByNameAllowNull(String name) {
        roleReadLock();
        try {
            return roleNameToId.get(name);
        } finally {
            roleReadUnlock();
        }
    }

    protected Long getRoleIdByNameNoLock(String name) throws PrivilegeException {
        Long roleId = roleNameToId.get(name);
        if (roleId == null) {
            throw new PrivilegeException(String.format("Role %s doesn't exist!", name));
        }
        return roleId;
    }

    public PEntryObject generateObject(ObjectType objectType, List<String> objectTokenList)
            throws PrivilegeException {
        if (objectTokenList == null) {
            return null;
        }
        return this.provider.generateObject(objectType, objectTokenList);
    }

    public PEntryObject generateUserObject(ObjectType objectType, UserIdentity user) throws PrivilegeException {
        return this.provider.generateUserObject(objectType, user);
    }

    public PEntryObject generateFunctionObject(ObjectType objectType, Long databaseId, Long functionId)
            throws PrivilegeException {
        return this.provider.generateFunctionObject(objectType, databaseId, functionId);
    }

    /**
     * remove invalid object periodically
     * <p>
     * lock order should always be:
     * `AuthenticationManager.lock` -> `AuthorizationManager.userLock` -> `AuthorizationManager.roleLock`
     */
    public void removeInvalidObject() {
        userWriteLock();
        try {
            // 1. remove invalidate object of users
            for (Map.Entry<UserIdentity, UserPrivilegeCollectionV2> userPrivEntry : userToPrivilegeCollection.entrySet()) {
                userPrivEntry.getValue().removeInvalidObject();
            }

            // 2. remove invalidate roles of users
            roleReadLock();
            try {
                for (Map.Entry<UserIdentity, UserPrivilegeCollectionV2> userPrivEntry : userToPrivilegeCollection.entrySet()) {
                    removeInvalidRolesUnlocked(userPrivEntry.getValue().getAllRoles());
                    removeInvalidRolesUnlocked(userPrivEntry.getValue().getDefaultRoleIds());
                }
            } finally {
                roleReadUnlock();
            }
        } finally {
            userWriteUnlock();
        }

        // 3. remove invalidate object of roles
        // we have to add user lock first because it may contain user privilege
        userReadLock();
        try {
            roleWriteLock();
            try {
                for (Map.Entry<Long, RolePrivilegeCollectionV2> rolePrivEntry : roleIdToPrivilegeCollection.entrySet()) {
                    rolePrivEntry.getValue().removeInvalidObject();
                }
            } finally {
                roleWriteUnlock();
            }
        } finally {
            userReadUnlock();
        }

        // 4. remove invalidate parent roles & sub roles
        roleWriteLock();
        try {
            for (Map.Entry<Long, RolePrivilegeCollectionV2> rolePrivEntry : roleIdToPrivilegeCollection.entrySet()) {
                RolePrivilegeCollectionV2 collection = rolePrivEntry.getValue();
                removeInvalidRolesUnlocked(collection.getParentRoleIds());
                removeInvalidRolesUnlocked(collection.getSubRoleIds());
            }
        } finally {
            roleWriteUnlock();
        }
    }

    private void removeInvalidRolesUnlocked(Set<Long> roleIds) {
        roleIds.removeIf(aLong -> !roleIdToPrivilegeCollection.containsKey(aLong));
    }

    /**
     * get max role inheritance depth
     * e.g. grant role_a to role role_b; grant role_b to role role_c;
     * then the inheritance graph would be role_a -> role_b -> role_c
     * the role inheritance depth for role_a would be 2, for role_b would be 1, for role_c would be 0
     */
    protected long getMaxRoleInheritanceDepthInner(long currentDepth, long roleId) throws PrivilegeException {
        RolePrivilegeCollectionV2 collection = getRolePrivilegeCollectionUnlocked(roleId, false);
        if (collection == null) {  // this role has been dropped
            return currentDepth - 1;
        }
        Set<Long> subRoleIds = collection.getSubRoleIds();
        if (subRoleIds.isEmpty()) {
            return currentDepth;
        } else {
            long maxDepth = -1;
            for (long subRoleId : subRoleIds) {
                // return the max depth
                maxDepth = Math.max(maxDepth, getMaxRoleInheritanceDepthInner(currentDepth + 1, subRoleId));
            }
            return maxDepth;
        }
    }

    /**
     * get all descendants roles(sub roles and their subs etc.)
     * e.g. grant role_a to role role_b; grant role_b to role role_c;
     * then the inheritance graph would be role_a -> role_b -> role_c
     * then all descendants roles of role_a would be [role_b, role_c]
     */
    protected Set<Long> getAllDescendantsUnlocked(long roleId) throws PrivilegeException {
        Set<Long> set = new HashSet<>();
        set.add(roleId);
        getAllDescendantsUnlockedInner(roleId, set);
        return set;
    }

    protected void getAllDescendantsUnlockedInner(long roleId, Set<Long> resultSet) throws PrivilegeException {
        RolePrivilegeCollectionV2 collection = getRolePrivilegeCollectionUnlocked(roleId, false);
        // this role has been dropped, but we still count it as descendants
        if (collection == null) {
            return;
        }
        for (Long subId : collection.getSubRoleIds()) {
            if (!resultSet.contains(subId)) {
                resultSet.add(subId);
                // recursively collect all predecessors
                getAllDescendantsUnlockedInner(subId, resultSet);
            }
        }
    }

    /**
     * get all predecessors roles (parent roles and their parents etc.)
     * e.g. grant role_a to role role_b; grant role_b to role role_c;
     * then the inheritance graph would be role_a -> role_b -> role_c
     * then all parent roles of role_c would be [role_a, role_b]
     */
    protected Set<Long> getAllPredecessorRoleIdsUnlocked(UserPrivilegeCollectionV2 collection) throws PrivilegeException {
        return getAllPredecessorRoleIdsUnlocked(collection.getAllRoles());
    }

    protected Set<Long> getAllPredecessorRoleIdsUnlocked(long roleId) throws PrivilegeException {
        Set<Long> set = new HashSet<>();
        set.add(roleId);
        return getAllPredecessorRoleIdsUnlocked(set);
    }

    protected Set<Long> getAllPredecessorRoleIdsUnlocked(Set<Long> initialRoleIds) throws PrivilegeException {
        Set<Long> result = new HashSet<>(initialRoleIds);
        for (long roleId : initialRoleIds) {
            getAllPredecessorRoleIdsInner(roleId, result);
        }
        return result;
    }

    protected void getAllPredecessorRoleIdsInner(long roleId, Set<Long> resultSet) throws PrivilegeException {
        RolePrivilegeCollectionV2 collection = getRolePrivilegeCollectionUnlocked(roleId, false);
        if (collection == null) { // this role has been dropped
            resultSet.remove(roleId);
            return;
        }
        for (Long parentId : collection.getParentRoleIds()) {
            if (!resultSet.contains(parentId)) {
                resultSet.add(parentId);
                // recursively collect all predecessors
                getAllPredecessorRoleIdsInner(parentId, resultSet);
            }
        }
    }

    public Set<String> getAllPredecessorRoleNames(Long roleId) {
        Set<Long> resultSetIds = new HashSet<>();
        resultSetIds.add(roleId);

        roleReadLock();
        try {
            getAllPredecessorRoleIdsInner(roleId, resultSetIds);
        } catch (PrivilegeException e) {
            // can ignore
        } finally {
            roleReadUnlock();
        }

        return new HashSet<>(getRoleNamesByRoleIds(resultSetIds));
    }

    public Set<String> getAllPredecessorRoleNamesByUser(UserIdentity userIdentity) throws PrivilegeException {
        Set<String> resultSet = new HashSet<>();
        getRoleIdsByUser(userIdentity).forEach(roleId -> resultSet.addAll(getAllPredecessorRoleNames(roleId)));
        return resultSet;
    }

    public boolean isLoaded() {
        return isLoaded;
    }

    public void setLoaded(boolean loaded) {
        isLoaded = loaded;
    }

    public void grantStorageVolumeUsageToPublicRole(String storageVolumeId) throws PrivilegeException {
        List<PEntryObject> object = Collections.singletonList(new StorageVolumePEntryObject(storageVolumeId));
        grantToRole(ObjectType.STORAGE_VOLUME, Collections.singletonList(PrivilegeType.USAGE), object, false,
                PrivilegeBuiltinConstants.PUBLIC_ROLE_NAME);
    }

    public void loadV2(SRMetaBlockReader reader) throws IOException, SRMetaBlockException, SRMetaBlockEOFException {
        // 1 json for myself
        AuthorizationMgr ret = reader.readJson(AuthorizationMgr.class);
        ret.provider = Objects.requireNonNullElseGet(provider, DefaultAuthorizationProvider::new);
        ret.initBuiltinRolesAndUsers();

        LOG.info("loading users");
        reader.readMap(UserIdentity.class, UserPrivilegeCollectionV2.class,
                (MapEntryConsumer<UserIdentity, UserPrivilegeCollectionV2>) (userIdentity, collection) -> {
                    if (userIdentity.equals(UserIdentity.ROOT)) {
                        try {
                            UserPrivilegeCollectionV2 rootUserPrivCollection =
                                    ret.getUserPrivilegeCollectionUnlocked(UserIdentity.ROOT);
                            collection.grantRoles(rootUserPrivCollection.getAllRoles());
                            collection.setDefaultRoleIds(rootUserPrivCollection.getDefaultRoleIds());
                            collection.typeToPrivilegeEntryList = rootUserPrivCollection.typeToPrivilegeEntryList;
                        } catch (PrivilegeException e) {
                            throw new IOException("failed to load users in AuthorizationManager!", e);
                        }
                    }

                    ret.userToPrivilegeCollection.put(userIdentity, collection);
                });

        LOG.info("loading roles");
        reader.readMap(Long.class, RolePrivilegeCollectionV2.class,
                (MapEntryConsumer<Long, RolePrivilegeCollectionV2>) (roleId, collection) -> {
                    // Use hard-code PrivilegeCollection in the memory as the built-in role permission.
                    // The reason why need to replay from the image here
                    // is because the associated information of the role-id is stored in the image.
                    if (PrivilegeBuiltinConstants.IMMUTABLE_BUILT_IN_ROLE_IDS.contains(roleId)) {
                        RolePrivilegeCollectionV2 builtInRolePrivilegeCollection =
                                ret.roleIdToPrivilegeCollection.get(roleId);
                        collection.typeToPrivilegeEntryList = builtInRolePrivilegeCollection.typeToPrivilegeEntryList;
                    }
                    ret.roleIdToPrivilegeCollection.put(roleId, collection);
                });

        LOG.info("loaded {} users, {} roles",
                ret.userToPrivilegeCollection.size(), ret.roleIdToPrivilegeCollection.size());

        // mark data is loaded
        isLoaded = true;
        roleNameToId = ret.roleNameToId;
        pluginId = ret.pluginId;
        pluginVersion = ret.pluginVersion;
        userToPrivilegeCollection = ret.userToPrivilegeCollection;
        roleIdToPrivilegeCollection = ret.roleIdToPrivilegeCollection;

        // Initialize the Authorizer class in advance during the loading phase
        // to prevent loading errors and lack of permissions.
        Authorizer.getInstance();
    }

    public void saveV2(ImageWriter imageWriter) throws IOException {
        try {
            // 1 json for myself,1 json for number of users, 2 json for each user(kv)
            // 1 json for number of roles, 2 json for each role(kv)
            final int cnt = 1 + 1 + userToPrivilegeCollection.size() * 2
                    + 1 + roleIdToPrivilegeCollection.size() * 2;
            SRMetaBlockWriter writer = imageWriter.getBlockWriter(SRMetaBlockID.AUTHORIZATION_MGR, cnt);
            // 1 json for myself
            writer.writeJson(this);
            // 1 json for num user
            writer.writeInt(userToPrivilegeCollection.size());
            for (Map.Entry<UserIdentity, UserPrivilegeCollectionV2> entry : userToPrivilegeCollection.entrySet()) {
                writer.writeJson(entry.getKey());
                writer.writeJson(entry.getValue());
            }
            // 1 json for num roles
            writer.writeInt(roleIdToPrivilegeCollection.size());
            for (Map.Entry<Long, RolePrivilegeCollectionV2> entry : roleIdToPrivilegeCollection.entrySet()) {
                RolePrivilegeCollectionV2 value = entry.getValue();
                // Avoid newly added PEntryObject type corrupt forward compatibility,
                // since built-in roles are always initialized on startup, we don't need to persist them.
                // But to keep the correct relationship with roles inherited from them, we still need to persist
                // an empty role for them, just for the role id.
                if (PrivilegeBuiltinConstants.IMMUTABLE_BUILT_IN_ROLE_IDS.contains(entry.getKey())) {
                    // clone to avoid race condition
                    RolePrivilegeCollectionV2 clone = value.cloneSelf();
                    clone.typeToPrivilegeEntryList = new HashMap<>();
                    value = clone;
                }
                writer.writeLong(entry.getKey());
                writer.writeJson(value);
            }
            writer.close();
        } catch (SRMetaBlockException e) {
            throw new IOException("failed to save AuthenticationManager!", e);
        }
    }

    // get all role ids of the user, including the default roles and the inactivated roles
    public Set<Long> getAllRoleIds(UserIdentity user) throws PrivilegeException {
        return getRoleIdsByUser(user);
    }

    // get the list of roles for the current user
    public List<RolePrivilegeCollectionV2> getApplicableRoles(UserIdentity user) throws PrivilegeException {
        List<RolePrivilegeCollectionV2> applicableRoles = new ArrayList<>();
        userReadLock();
        try {
            Set<Long> roleIds = getRoleIdsByUser(user);
            roleReadLock();
            try {
                for (Long roleId : roleIds) {
                    RolePrivilegeCollectionV2 rolePrivilegeCollection = roleIdToPrivilegeCollection.get(roleId);
                    if (rolePrivilegeCollection != null) {
                        applicableRoles.add(rolePrivilegeCollection);
                    }
                }
            } finally {
                roleReadUnlock();
            }
        } finally {
            userReadUnlock();
        }
        return applicableRoles;
    }
}
