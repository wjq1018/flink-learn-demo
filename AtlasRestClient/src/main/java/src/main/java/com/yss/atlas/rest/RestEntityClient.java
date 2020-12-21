package src.main.java.com.yss.atlas.rest;

import com.google.common.collect.Lists;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasException;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.instance.*;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.commons.collections.CollectionUtils;
import src.main.java.com.yss.atlas.rest.client.AtlasClientV2Builder;
import src.main.java.com.yss.atlas.rest.def.types.AtlasEntityDefBuilder;

import java.util.*;

import static java.util.Arrays.asList;
import static org.apache.atlas.type.AtlasTypeUtil.*;
import static org.apache.atlas.type.AtlasTypeUtil.toAtlasRelatedObjectIds;

/**
 * rest操作实体-客户端
 *
 * @author daomingzhu
 * @date 2020/7/1
 */
public class RestEntityClient {
    public static void main(String[] args) throws Exception {
        //创建客户端连接
        AtlasClientV2 atlasClient = AtlasClientV2Builder.create()
                .urls(new String[]{"http://172.18.16.33:21000/"})
                .basicAuth("admin", "admin")
                .build();
        //实体类型
//        AtlasTypesDef atlasTypesDef = AtlasTypesDefBuilder.create()
//                .entityDef(
//                        createClassTypeDef(
//                        "DD",
//                        Collections.singleton("DataSet"),
//                        createUniqueRequiredAttrDef("name", "string"),
//                        createOptionalAttrDef("description", "string"))
//                )
//                .build();
//        atlasClient.createAtlasTypeDefs(atlasTypesDef);
        //创建实体 血缘
        AtlasEntity entityInput = AtlasEntityDefBuilder.create()
                .typename("DD")
                .version("1.0")
                .mapAttributes(new HashMap<String, String>(2) {
                    {
                        put("name", "sdww");
                    }

                    {
                        put("description", "scvvcccccc");
                    }

                    {
                        put("qualifiedName", "啛啛喳喳");
                    }
                })
                .build();
        AtlasEntity finalEntityInput = createEntity(atlasClient, new AtlasEntity.AtlasEntityWithExtInfo(entityInput));

        AtlasEntity entityOut = AtlasEntityDefBuilder.create()
                .typename("DD")
                .version("1.0")
                .mapAttributes(new HashMap<String, String>(2) {
                    {
                        put("name", "asdasdasd");
                    }

                    {
                        put("description", "zzxx");
                    }

                    {
                        put("qualifiedName", "按时撒所");
                    }
                })
                .build();
        AtlasEntity finalEntityOut = createEntity(atlasClient, new AtlasEntity.AtlasEntityWithExtInfo(entityOut));

        AtlasEntity process = AtlasEntityDefBuilder.create()
                .typename("Process")
                .version("1.0")
                .relationshipAttributes(new HashMap<String, Object>(2) {
                    {
                        put("inputs", toAtlasRelatedObjectIds(Lists.newArrayList(finalEntityInput)));
                    }

                    {
                        put("outputs", toAtlasRelatedObjectIds(Lists.newArrayList(finalEntityOut)));
                    }
                })
                .mapAttributes(new HashMap<String, String>(2) {
                    {
                        put("name", "ceshi_process");
                    }

                    {
                        put("description", "ces description");
                    }

                    {
                        put("qualifiedName", "ceshi_process@a1");
                    }
                })
                .build();
        atlasClient.createEntity(new AtlasEntity.AtlasEntityWithExtInfo(process));

        //查询DSL血缘
    }

    private static AtlasEntity  createEntity(AtlasClientV2 atlasClientV2, AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo) throws Exception {
        AtlasEntity ret = null;
        EntityMutationResponse response = atlasClientV2.createEntity(entityWithExtInfo);
        String gid = response.getGuidAssignments().get(entityWithExtInfo.getEntity().getGuid());

        if (Objects.nonNull(gid)) {
            AtlasEntity.AtlasEntityWithExtInfo getByGuidResponse = atlasClientV2.getEntityByGuid(gid);

            ret = getByGuidResponse.getEntity();

            System.out.println("Created entity of type [" + ret.getTypeName() + "], guid: " + ret.getGuid());
        }

        return ret;
    }

    /**
     * 分配标签
     *
     * @param classificationNames
     * @return
     */
    static List<AtlasClassification> toAtlasClassifications(String[] classificationNames) {
        List<AtlasClassification> ret = new ArrayList();
        List<String> classifications = asList(classificationNames);
        for (String classificationName : classifications) {
            ret.add(new AtlasClassification(classificationName));
        }

        return ret;
    }

    /**
     * 创建类型
     *
     * @return
     */
    static AtlasTypesDef testCreateTypeDef() {
        AtlasEntityDef dbTypeDef = createClassTypeDef("TEST", "TEST", "1.0", Collections.singleton("DataSet"),
                createUniqueRequiredAttrDef("name", "string"),
                createOptionalAttrDef("description", "string"),
                createOptionalAttrDef("locationUri", "string"),
                createOptionalAttrDef("owner", "string"),
                createOptionalAttrDef("createTime", "long"));

        return null;
    }
}