package src.main.java.com.yss.atlas.rest.def.types;

import com.google.common.collect.Lists;
import org.apache.atlas.model.typedef.*;

import java.util.List;

/**
 * atlas类型定义建造器
 *
 * @author daomingzhu
 * @date 2020/7/2
 */
public class AtlasTypesDefBuilder {

    private List<AtlasEnumDef> enumDefs = Lists.newArrayList();
    private List<AtlasStructDef> structDefs = Lists.newArrayList();
    private List<AtlasClassificationDef> classificationDefs = Lists.newArrayList();
    private List<AtlasEntityDef> entityDefs = Lists.newArrayList();
    private List<AtlasRelationshipDef> relationshipDefs = Lists.newArrayList();
    private List<AtlasBusinessMetadataDef> businessMetadataDefs = Lists.newArrayList();

    /**
     * 定义枚举
     * @param enumDef 枚举
     * @return AtlasTypesDefBuilder
     */
    public AtlasTypesDefBuilder enumDef(AtlasEnumDef enumDef){
        this.enumDefs = Lists.newArrayList(enumDef);
        return this;
    }
    public AtlasTypesDefBuilder enumDefs(List<AtlasEnumDef> enumDefs){
        this.enumDefs = enumDefs;
        return this;
    }
    /**
     * 定义结构
     * @param structDef 实体结构
     * @return AtlasTypesDefBuilder
     */
    public AtlasTypesDefBuilder structDef(AtlasStructDef structDef){
        this.structDefs = Lists.newArrayList(structDef);
        return this;
    }
    public AtlasTypesDefBuilder structDefs(List<AtlasStructDef> structDefs){
        this.structDefs = structDefs;
        return this;
    }

    /**
     * 定义标签
     * @param classificationDef 标签定义
     * @return AtlasTypesDefBuilder
     */
    public AtlasTypesDefBuilder classificationDef(AtlasClassificationDef classificationDef){
        this.classificationDefs = Lists.newArrayList(classificationDef);
        return this;
    }
    public AtlasTypesDefBuilder classificationDefs(List<AtlasClassificationDef> classificationDefs){
        this.classificationDefs = classificationDefs;
        return this;
    }
    /**
     * 定义实体类型
     * @param entityDef 标签定义
     * @return AtlasTypesDefBuilder
     */
    public AtlasTypesDefBuilder entityDef(AtlasEntityDef entityDef){
        this.entityDefs = Lists.newArrayList(entityDef);
        return this;
    }
    public AtlasTypesDefBuilder entityDefs(List<AtlasEntityDef> entityDefs){
        this.entityDefs = entityDefs;
        return this;
    }

    /**
     * 定义关系
     * @param relationshipDef 关系定义
     * @return AtlasTypesDefBuilder
     */
    public AtlasTypesDefBuilder relationshipDef(AtlasRelationshipDef relationshipDef){
        this.relationshipDefs = Lists.newArrayList(relationshipDefs);
        return this;
    }
    public AtlasTypesDefBuilder relationshipDefs(List<AtlasRelationshipDef> relationshipDefs){
        this.relationshipDefs = relationshipDefs;
        return this;
    }
    /**
     * 定义业务元数据
     * @param businessMetadataDef 关系定义
     * @return AtlasTypesDefBuilder
     */
    public AtlasTypesDefBuilder businessMetadataDef(AtlasBusinessMetadataDef businessMetadataDef){
        this.businessMetadataDefs = Lists.newArrayList(businessMetadataDefs);
        return this;
    }
    public AtlasTypesDefBuilder businessMetadataDefs(List<AtlasBusinessMetadataDef> businessMetadataDefs){
        this.businessMetadataDefs = businessMetadataDefs;
        return this;
    }

    public AtlasTypesDef build() {
        return new AtlasTypesDef(
                enumDefs,
                structDefs,
                classificationDefs,
                entityDefs,
                relationshipDefs,
                businessMetadataDefs);
    }

    public static AtlasTypesDefBuilder create() {
        return new AtlasTypesDefBuilder();
    }
}
