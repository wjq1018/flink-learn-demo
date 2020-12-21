package src.main.java.com.yss.datamiddle.testHook;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.hook.AtlasHook;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.notification.HookNotification;
import org.apache.commons.configuration.Configuration;

import java.util.Collections;

public class TestHook extends AtlasHook {
    public static void main(String[] args) throws AtlasException {
        new TestHook().sendNotification();
    }

    private void sendNotification() throws AtlasException {
        Configuration atlasProperties = ApplicationProperties.get();
        AtlasEntity.AtlasEntitiesWithExtInfo entities = new AtlasEntity.AtlasEntitiesWithExtInfo();
        HookNotification message = new HookNotification.EntityCreateRequestV2(AtlasHook.getUser(),
                entities);
        notifyEntities(Collections.singletonList(message), null);
    }
}