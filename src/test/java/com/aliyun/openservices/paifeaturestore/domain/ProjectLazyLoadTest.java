package com.aliyun.openservices.paifeaturestore.domain;

import com.aliyun.openservices.paifeaturestore.api.ApiClient;
import com.aliyun.openservices.paifeaturestore.api.Configuration;
import com.aliyun.openservices.paifeaturestore.api.FeatureEntityApi;
import com.aliyun.openservices.paifeaturestore.api.FeatureViewApi;
import com.aliyun.openservices.paifeaturestore.api.FsModelApi;
import com.aliyun.openservices.paifeaturestore.api.ListFeatureEntitiesResponse;
import com.aliyun.openservices.paifeaturestore.api.ListFeatureViewsResponse;
import com.aliyun.openservices.paifeaturestore.api.ListModesResponse;
import com.aliyun.openservices.paifeaturestore.constants.DatasourceType;
import com.aliyun.openservices.paifeaturestore.constants.FSType;
import com.aliyun.openservices.paifeaturestore.datasource.FeatureDBClient;
import com.aliyun.openservices.paifeaturestore.datasource.FeatureDBFactory;
import com.aliyun.openservices.paifeaturestore.datasource.HttpConfig;
import com.aliyun.openservices.paifeaturestore.model.Datasource;
import com.aliyun.openservices.paifeaturestore.model.FeatureViewRequestFields;
import com.aliyun.openservices.paifeaturestore.model.ModelFeatures;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ProjectLazyLoadTest {

    private static final String TEST_FDB_NAME = "test_lazy_load_fdb";
    private static final Long PROJECT_ID = 100001L;
    private static final String PROJECT_NAME = "test_project";

    @BeforeClass
    public static void setUpClass() {
        FeatureDBFactory.register(TEST_FDB_NAME, new FeatureDBClient(new HttpConfig()));
    }

    private com.aliyun.openservices.paifeaturestore.model.Project buildModelProject() {
        Datasource onlineDs = new Datasource();
        onlineDs.setName(TEST_FDB_NAME);
        onlineDs.setType(DatasourceType.Datasource_Type_FeatureDB);

        com.aliyun.openservices.paifeaturestore.model.Project p = new com.aliyun.openservices.paifeaturestore.model.Project();
        p.setProjectId(PROJECT_ID);
        p.setProjectName(PROJECT_NAME);
        p.setInstanceId("test_instance");
        p.setOnlineDatasourceType(DatasourceType.Datasource_Type_FeatureDB);
        p.setOnlineDataSource(onlineDs);
        p.createSignature("test_user", "test_pass");
        return p;
    }

    private com.aliyun.openservices.paifeaturestore.model.FeatureView buildFeatureViewModel(
            long id, String name, String entityName) {
        com.aliyun.openservices.paifeaturestore.model.FeatureView fv = new com.aliyun.openservices.paifeaturestore.model.FeatureView();
        fv.setFeatureViewId(id);
        fv.setName(name);
        fv.setType("Batch");
        fv.setProjectId(PROJECT_ID);
        fv.setProjectName(PROJECT_NAME);
        fv.setFeatureEntityName(entityName);
        fv.setRegisterDatasourceId(0);
        fv.setWriteToFeaturedb(true);
        fv.setTtl(-1);

        FeatureViewRequestFields pk = new FeatureViewRequestFields();
        pk.setName("user_id");
        pk.setType(FSType.FS_STRING);
        pk.setIsPrimaryKey(true);
        pk.setPosition(1);

        FeatureViewRequestFields f1 = new FeatureViewRequestFields();
        f1.setName("age");
        f1.setType(FSType.FS_INT32);
        f1.setPosition(2);

        fv.setFields(Arrays.asList(pk, f1));
        return fv;
    }

    private com.aliyun.openservices.paifeaturestore.model.FeatureEntity buildEntityModel(int id, String name, String joinId) {
        com.aliyun.openservices.paifeaturestore.model.FeatureEntity e = new com.aliyun.openservices.paifeaturestore.model.FeatureEntity();
        e.setFeatureEntityId(id);
        e.setFeatureEntityName(name);
        e.setFeatureEntityJoinid(joinId);
        e.setProjectId(PROJECT_ID);
        e.setProjectName(PROJECT_NAME);
        return e;
    }

    private static class StubApiClient extends ApiClient {
        private final FeatureViewApi featureViewApi;
        private final FeatureEntityApi featureEntityApi;
        private final FsModelApi fsModelApi;

        StubApiClient(FeatureViewApi featureViewApi, FeatureEntityApi featureEntityApi, FsModelApi fsModelApi) throws Exception {
            super(new Configuration("cn-hangzhou", "test_ak", "test_sk", PROJECT_NAME));
            this.featureViewApi = featureViewApi;
            this.featureEntityApi = featureEntityApi;
            this.fsModelApi = fsModelApi;
        }

        @Override
        public FeatureViewApi getFeatureViewApi() {
            return featureViewApi;
        }

        @Override
        public FeatureEntityApi getFeatureEntityApi() {
            return featureEntityApi;
        }

        @Override
        public FsModelApi getFsModelApi() {
            return fsModelApi;
        }
    }

    @Test
    public void testFuzzyMatchFiltersToExactFeatureViewAndCachesHit() throws Exception {
        AtomicInteger listCount = new AtomicInteger();
        AtomicInteger getByIdCount = new AtomicInteger();

        FeatureViewApi fvApi = new FeatureViewApi(null) {
            @Override
            public ListFeatureViewsResponse listFeatureViewsByName(String featureViewName, String projectId, int pageNumber, int pageSize) {
                listCount.incrementAndGet();
                ListFeatureViewsResponse resp = new ListFeatureViewsResponse();
                resp.setTotalCount(3L);

                com.aliyun.openservices.paifeaturestore.model.FeatureView broken = new com.aliyun.openservices.paifeaturestore.model.FeatureView();
                broken.setFeatureViewId(1L);
                broken.setName("user_fea_broken");

                com.aliyun.openservices.paifeaturestore.model.FeatureView target = new com.aliyun.openservices.paifeaturestore.model.FeatureView();
                target.setFeatureViewId(2L);
                target.setName("user_fea");

                com.aliyun.openservices.paifeaturestore.model.FeatureView sibling = new com.aliyun.openservices.paifeaturestore.model.FeatureView();
                sibling.setFeatureViewId(3L);
                sibling.setName("user_fea_v2");

                resp.setFeatureViews(Arrays.asList(broken, target, sibling));
                return resp;
            }

            @Override
            public com.aliyun.openservices.paifeaturestore.model.FeatureView getFeatureViewById(String featureViewId) {
                getByIdCount.incrementAndGet();
                if ("2".equals(featureViewId)) {
                    return buildFeatureViewModel(2L, "user_fea", "user_entity");
                }
                throw new AssertionError("Should not call getFeatureViewById for non-matching id: " + featureViewId);
            }
        };

        FeatureEntityApi entityApi = new FeatureEntityApi(null) {
            @Override
            public ListFeatureEntitiesResponse listFeatureEntities(String projectId, int pageNumber, int pageSize) {
                ListFeatureEntitiesResponse resp = new ListFeatureEntitiesResponse();
                resp.setTotalCount(1);
                resp.setFeatureEntities(Collections.singletonList(buildEntityModel(1, "user_entity", "user_id")));
                return resp;
            }
        };

        Project project = new Project(buildModelProject(), false, new StubApiClient(fvApi, entityApi, new FsModelApi(null)));

        FeatureView fv = project.getFeatureView("user_fea");
        assertNotNull(fv);
        assertEquals("user_fea", fv.getName());
        assertEquals(1, listCount.get());
        assertEquals(1, getByIdCount.get());

        // Cached hit: no additional RPC
        FeatureView fv2 = project.getFeatureView("user_fea");
        assertSame(fv, fv2);
        assertEquals(1, listCount.get());
        assertEquals(1, getByIdCount.get());
    }

    @Test
    public void testFuzzyMatchFiltersToExactModelAndAvoidsBrokenSibling() throws Exception {
        AtomicInteger modelGetByIdCount = new AtomicInteger();

        FeatureViewApi fvApi = new FeatureViewApi(null) {
            @Override
            public ListFeatureViewsResponse listFeatureViewsByName(String featureViewName, String projectId, int pageNumber, int pageSize) {
                ListFeatureViewsResponse resp = new ListFeatureViewsResponse();
                resp.setTotalCount(1L);
                com.aliyun.openservices.paifeaturestore.model.FeatureView target = new com.aliyun.openservices.paifeaturestore.model.FeatureView();
                target.setFeatureViewId(10L);
                target.setName("user_fea");
                resp.setFeatureViews(Collections.singletonList(target));
                return resp;
            }

            @Override
            public com.aliyun.openservices.paifeaturestore.model.FeatureView getFeatureViewById(String featureViewId) {
                return buildFeatureViewModel(10L, "user_fea", "user_entity");
            }
        };

        FeatureEntityApi entityApi = new FeatureEntityApi(null) {
            @Override
            public ListFeatureEntitiesResponse listFeatureEntities(String projectId, int pageNumber, int pageSize) {
                ListFeatureEntitiesResponse resp = new ListFeatureEntitiesResponse();
                resp.setTotalCount(1);
                resp.setFeatureEntities(Collections.singletonList(buildEntityModel(1, "user_entity", "user_id")));
                return resp;
            }
        };

        FsModelApi modelApi = new FsModelApi(null) {
            @Override
            public ListModesResponse listModelsByName(String modelFeatureName, String projectId, int pageNumber, int pageSize) {
                ListModesResponse resp = new ListModesResponse();
                resp.setTotalCount(2L);

                com.aliyun.openservices.paifeaturestore.model.Model broken = new com.aliyun.openservices.paifeaturestore.model.Model();
                broken.setModelId(100L);
                broken.setName("rank_broken");

                com.aliyun.openservices.paifeaturestore.model.Model target = new com.aliyun.openservices.paifeaturestore.model.Model();
                target.setModelId(101L);
                target.setName("rank");

                resp.setModels(Arrays.asList(broken, target));
                return resp;
            }

            @Override
            public com.aliyun.openservices.paifeaturestore.model.Model getModelById(String modelId) {
                modelGetByIdCount.incrementAndGet();
                if (!"101".equals(modelId)) {
                    throw new AssertionError("Should not load non-matching model id: " + modelId);
                }
                com.aliyun.openservices.paifeaturestore.model.Model m = new com.aliyun.openservices.paifeaturestore.model.Model();
                m.setModelId(101L);
                m.setName("rank");
                m.setProjectId(PROJECT_ID);
                m.setProjectName(PROJECT_NAME);
                ModelFeatures mf = new ModelFeatures();
                mf.setFeatureViewName("user_fea");
                mf.setName("age");
                mf.setType(FSType.FS_INT32);
                m.setFeatures(Collections.singletonList(mf));
                return m;
            }
        };

        Project project = new Project(buildModelProject(), false, new StubApiClient(fvApi, entityApi, modelApi));
        Model model = project.getModel("rank");
        assertNotNull(model);
        assertEquals("rank", model.getModel().getName());
        assertEquals(1, modelGetByIdCount.get());
    }

    @Test
    public void testNullNamesReturnNullWithoutApiCallsOrNpe() throws Exception {
        AtomicInteger apiCalls = new AtomicInteger();

        FeatureViewApi fvApi = new FeatureViewApi(null) {
            @Override
            public ListFeatureViewsResponse listFeatureViewsByName(String featureViewName, String projectId, int pageNumber, int pageSize) {
                apiCalls.incrementAndGet();
                return new ListFeatureViewsResponse();
            }
        };
        FeatureEntityApi entityApi = new FeatureEntityApi(null) {
            @Override
            public ListFeatureEntitiesResponse listFeatureEntities(String projectId, int pageNumber, int pageSize) {
                apiCalls.incrementAndGet();
                return new ListFeatureEntitiesResponse();
            }
        };
        FsModelApi modelApi = new FsModelApi(null) {
            @Override
            public ListModesResponse listModelsByName(String modelFeatureName, String projectId, int pageNumber, int pageSize) {
                apiCalls.incrementAndGet();
                return new ListModesResponse();
            }
        };

        Project project = new Project(buildModelProject(), false, new StubApiClient(fvApi, entityApi, modelApi));

        assertNull(project.getFeatureView(null));
        assertNull(project.getSeqFeatureView(null));
        assertNull(project.getFeatureEntity(null));
        assertNull(project.getModel(null));
        assertEquals(0, apiCalls.get());
    }

    @Test
    public void testNullAndEmptyResponsesReturnNullAndEntitiesDoNotReload() throws Exception {
        AtomicInteger entityListCalls = new AtomicInteger();

        FeatureViewApi fvApi = new FeatureViewApi(null) {
            @Override
            public ListFeatureViewsResponse listFeatureViewsByName(String featureViewName, String projectId, int pageNumber, int pageSize) {
                ListFeatureViewsResponse resp = new ListFeatureViewsResponse();
                resp.setTotalCount(null);
                resp.setFeatureViews(null);
                return resp;
            }
        };
        FeatureEntityApi entityApi = new FeatureEntityApi(null) {
            @Override
            public ListFeatureEntitiesResponse listFeatureEntities(String projectId, int pageNumber, int pageSize) {
                entityListCalls.incrementAndGet();
                ListFeatureEntitiesResponse resp = new ListFeatureEntitiesResponse();
                resp.setTotalCount(null);
                resp.setFeatureEntities(null);
                return resp;
            }
        };
        FsModelApi modelApi = new FsModelApi(null) {
            @Override
            public ListModesResponse listModelsByName(String modelFeatureName, String projectId, int pageNumber, int pageSize) {
                ListModesResponse resp = new ListModesResponse();
                resp.setTotalCount(null);
                resp.setModels(null);
                return resp;
            }
        };

        Project project = new Project(buildModelProject(), false, new StubApiClient(fvApi, entityApi, modelApi));

        assertNull(project.getFeatureView("non_existent"));
        assertNull(project.getSeqFeatureView("non_existent"));
        assertNull(project.getModel("non_existent"));
        assertNull(project.getFeatureEntity("non_existent_1"));
        assertNull(project.getFeatureEntity("non_existent_2"));
        assertEquals("Empty project entities should only be loaded once", 1, entityListCalls.get());
    }

    @Test
    public void testMissingEntityFailsFastWithoutCachingHalfInitializedView() throws Exception {
        FeatureViewApi fvApi = new FeatureViewApi(null) {
            @Override
            public ListFeatureViewsResponse listFeatureViewsByName(String featureViewName, String projectId, int pageNumber, int pageSize) {
                ListFeatureViewsResponse resp = new ListFeatureViewsResponse();
                resp.setTotalCount(1L);
                com.aliyun.openservices.paifeaturestore.model.FeatureView v = new com.aliyun.openservices.paifeaturestore.model.FeatureView();
                v.setFeatureViewId(9L);
                v.setName("fv_missing_entity");
                resp.setFeatureViews(Collections.singletonList(v));
                return resp;
            }

            @Override
            public com.aliyun.openservices.paifeaturestore.model.FeatureView getFeatureViewById(String featureViewId) {
                return buildFeatureViewModel(9L, "fv_missing_entity", "missing_entity");
            }
        };
        FeatureEntityApi entityApi = new FeatureEntityApi(null) {
            @Override
            public ListFeatureEntitiesResponse listFeatureEntities(String projectId, int pageNumber, int pageSize) {
                ListFeatureEntitiesResponse resp = new ListFeatureEntitiesResponse();
                resp.setTotalCount(0);
                resp.setFeatureEntities(Collections.emptyList());
                return resp;
            }
        };

        Project project = new Project(buildModelProject(), false, new StubApiClient(fvApi, entityApi, new FsModelApi(null)));
        try {
            project.getFeatureView("fv_missing_entity");
            fail("Expected IllegalStateException");
        } catch (IllegalStateException e) {
            Throwable root = e.getCause() != null ? e.getCause() : e;
            assertTrue(root.getMessage().contains("missing_entity"));
        }
        assertTrue("Broken FeatureView must not be cached", project.getFeatureViewMap().isEmpty());
    }

    @Test
    public void testConcurrentGetFeatureViewLoadsOnlyOnce() throws Exception {
        AtomicInteger listCalls = new AtomicInteger();
        AtomicInteger getByIdCalls = new AtomicInteger();

        FeatureViewApi fvApi = new FeatureViewApi(null) {
            @Override
            public ListFeatureViewsResponse listFeatureViewsByName(String featureViewName, String projectId, int pageNumber, int pageSize) throws Exception {
                listCalls.incrementAndGet();
                Thread.sleep(50);
                ListFeatureViewsResponse resp = new ListFeatureViewsResponse();
                resp.setTotalCount(1L);
                com.aliyun.openservices.paifeaturestore.model.FeatureView v = new com.aliyun.openservices.paifeaturestore.model.FeatureView();
                v.setFeatureViewId(7L);
                v.setName("concurrent_fv");
                resp.setFeatureViews(Collections.singletonList(v));
                return resp;
            }

            @Override
            public com.aliyun.openservices.paifeaturestore.model.FeatureView getFeatureViewById(String featureViewId) {
                getByIdCalls.incrementAndGet();
                return buildFeatureViewModel(7L, "concurrent_fv", "user_entity");
            }
        };

        FeatureEntityApi entityApi = new FeatureEntityApi(null) {
            @Override
            public ListFeatureEntitiesResponse listFeatureEntities(String projectId, int pageNumber, int pageSize) {
                ListFeatureEntitiesResponse resp = new ListFeatureEntitiesResponse();
                resp.setTotalCount(1);
                resp.setFeatureEntities(Collections.singletonList(buildEntityModel(1, "user_entity", "user_id")));
                return resp;
            }
        };

        Project project = new Project(buildModelProject(), false, new StubApiClient(fvApi, entityApi, new FsModelApi(null)));
        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<FeatureView>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit((Callable<FeatureView>) () -> {
                    ready.countDown();
                    start.await();
                    return project.getFeatureView("concurrent_fv");
                }));
            }
            ready.await();
            start.countDown();

            FeatureView first = futures.get(0).get();
            assertNotNull(first);
            for (Future<FeatureView> f : futures) {
                assertSame(first, f.get());
            }
            assertEquals(1, listCalls.get());
            assertEquals(1, getByIdCalls.get());
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    public void testEntityPaginationStopsWhenTotalCountEqualsPageSize() throws Exception {
        AtomicInteger pageRequests = new AtomicInteger();

        FeatureEntityApi entityApi = new FeatureEntityApi(null) {
            @Override
            public ListFeatureEntitiesResponse listFeatureEntities(String projectId, int pageNumber, int pageSize) {
                pageRequests.incrementAndGet();
                List<com.aliyun.openservices.paifeaturestore.model.FeatureEntity> list = new ArrayList<>();
                for (int i = 0; i < pageSize; i++) {
                    list.add(buildEntityModel(i, "entity_" + i, "join_" + i));
                }
                ListFeatureEntitiesResponse resp = new ListFeatureEntitiesResponse();
                resp.setTotalCount(pageSize);
                resp.setFeatureEntities(list);
                return resp;
            }
        };

        Project project = new Project(buildModelProject(), false, new StubApiClient(new FeatureViewApi(null), entityApi, new FsModelApi(null)));
        assertNotNull(project.getFeatureEntity("entity_0"));
        assertNotNull(project.getFeatureEntity("entity_99"));
        assertEquals("Should not fetch an extra empty page when totalCount == pageSize", 1, pageRequests.get());
    }
}
