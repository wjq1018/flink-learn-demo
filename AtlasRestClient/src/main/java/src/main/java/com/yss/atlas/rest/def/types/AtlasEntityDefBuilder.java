package src.main.java.com.yss.atlas.rest.def.types;


import com.google.common.collect.Maps;
import org.apache.atlas.model.glossary.relations.AtlasTermAssignmentHeader;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 实体定义构建器
 *
 * @author daomingzhu
 * @date 2020/7/3
 */
public class AtlasEntityDefBuilder {
    private String typeName;
    private Map<String, Object> attributes;
    private Map<String, Object> map = Maps.newHashMap();
    private Map<String, Object> relationshipAttributes;
    private List<AtlasClassification> classifications;
    private List<AtlasTermAssignmentHeader> meanings;
    private Map<String, String> customAttributes;
    private Map<String, Map<String, Object>> businessAttributes;
    private Set<String> labels;

    /**
     * 配置实体属性：参考AtlasEntity，AtlasStruct 的常量key_xxxx
     * KEY_GUID            = "guid" value -> String
     * KEY_HOME_ID         = "homeId" value -> String
     * KEY_IS_PROXY        = "isProxy" value -> Boolean
     * KEY_IS_INCOMPLETE   = "isIncomplete"  value -> Boolean
     * KEY_PROVENANCE_TYPE = "provenanceType"  value -> Number
     * KEY_STATUS          = "status" value -> String
     * KEY_CREATED_BY      = "createdBy" value -> String
     * KEY_UPDATED_BY      = "updatedBy" value -> String
     * KEY_CREATE_TIME     = "createTime" value -> Number
     * KEY_UPDATE_TIME     = "updateTime" value -> Number
     * KEY_VERSION         = "version" value -> Number
     * KEY_TYPENAME   = "typeName" value -> String
     * KEY_ATTRIBUTES = "attributes" value -> Map
     *
     * @param key key使用常量key
     * @param val 值类型参考文档
     * @return
     */
    public AtlasEntityDefBuilder mapEntityAttr(String key, Object val) {
        this.map.put(key, val);
        return this;
    }

    /**
     * 映射技术元数据属性
     *
     * @param attributes 属性映射
     * @return
     */
    public AtlasEntityDefBuilder mapAttributes(Map attributes) {
        this.map.put(AtlasEntity.KEY_ATTRIBUTES, attributes);
        return this;
    }

    /**
     * 实体类型
     *
     * @param typename 类型名称
     * @return
     */
    public AtlasEntityDefBuilder typename(String typename) {
        this.map.put(AtlasEntity.KEY_TYPENAME, typename);
        return this;
    }
    public AtlasEntityDefBuilder version(String version) {
        this.map.put(AtlasEntity.KEY_VERSION, version);
        return this;
    }

    /**
     * 关系配置
     *
     * @param relationshipAttributes 关系属性信息
     * @return
     */
    public AtlasEntityDefBuilder relationshipAttributes(Map<String, Object> relationshipAttributes) {
        this.relationshipAttributes = relationshipAttributes;
        return this;
    }

    /**
     * 分类标签配置
     *
     * @param classifications 标签
     * @return
     */
    public AtlasEntityDefBuilder classifications(List<AtlasClassification> classifications) {
        this.classifications = classifications;
        return this;
    }

    public AtlasEntityDefBuilder meanings(List<AtlasTermAssignmentHeader> meanings) {
        this.meanings = meanings;
        return this;
    }

    /**
     * 自定义属性
     *
     * @param customAttributes 自定义属性
     * @return
     */
    public AtlasEntityDefBuilder customAttributes(Map<String, String> customAttributes) {
        this.customAttributes = customAttributes;
        return this;
    }

    /**
     * 业务属性信息配置
     *
     * @param businessAttributes 业务属性信息
     * @return
     */
    public AtlasEntityDefBuilder businessAttributes(Map<String, Map<String, Object>> businessAttributes) {
        this.businessAttributes = businessAttributes;
        return this;
    }

    public AtlasEntityDefBuilder labels(Set<String> labels) {
        this.labels = labels;
        return this;
    }


    public AtlasEntity build() {
        init();
        AtlasEntity atlasEntity = new AtlasEntity(map);
        atlasEntity.setClassifications(classifications);
        atlasEntity.setRelationshipAttributes(relationshipAttributes);
        atlasEntity.setCustomAttributes(customAttributes);
        atlasEntity.setBusinessAttributes(businessAttributes);
        atlasEntity.setLabels(labels);
        return atlasEntity;
    }

    private void init() {
        Objects.requireNonNull(map.get(AtlasEntity.KEY_TYPENAME));
        if (Objects.isNull(map.get(AtlasEntity.KEY_GUID))) {
            map.put(AtlasEntity.KEY_GUID, nextInternalId());
        }
        if (Objects.isNull(map.get(AtlasEntity.KEY_IS_PROXY))) {
            map.put(AtlasEntity.KEY_IS_PROXY, Boolean.FALSE);
        }
        if (Objects.isNull(map.get(AtlasEntity.KEY_IS_INCOMPLETE))) {
            map.put(AtlasEntity.KEY_IS_INCOMPLETE, Boolean.FALSE);
        }
    }

    public static AtlasEntityDefBuilder create() {
        return new AtlasEntityDefBuilder();
    }

    private static AtomicLong s_nextId = new AtomicLong(System.nanoTime());

    private static String nextInternalId() {
        return "-" + Long.toString(s_nextId.getAndIncrement());
    }
}
