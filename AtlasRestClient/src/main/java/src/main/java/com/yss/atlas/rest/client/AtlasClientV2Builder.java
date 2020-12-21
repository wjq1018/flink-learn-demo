package src.main.java.com.yss.atlas.rest.client;

import org.apache.atlas.AtlasClientV2;

/**
 * 构建atlas客户端连接的建造器
 * @author daomingzhu
 * @date 2020/7/2
 */
public class AtlasClientV2Builder {
    private String[] urls;
    private String[] basicAuthUserNamePassword;

    public AtlasClientV2Builder() {
    }

    public AtlasClientV2Builder basicAuth(String user,String password){
        this.basicAuthUserNamePassword = new String[]{user,password};
        return this;
    }
    public AtlasClientV2Builder basicAuth(String token){
        return this;
    }
    public AtlasClientV2Builder urls(String[] urls){
        this.urls = urls;
        return this;
    }
    public AtlasClientV2 build(){
        AtlasClientV2 atlasClientV2 = new AtlasClientV2(urls, basicAuthUserNamePassword);
        return atlasClientV2;
    }
    public static AtlasClientV2Builder create(){
        return new AtlasClientV2Builder();
    }
}
