package src.main.java.com.yss.datamiddle.janusGraphTest;

import org.apache.atlas.AtlasException;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.janus.AtlasJanusGraph;
import org.apache.atlas.repository.graphdb.janus.AtlasJanusGraphDatabase;
import org.apache.commons.configuration.Configuration;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;

public class JanusGraphTest {
    public static void main(String[] args) throws AtlasBaseException {
        JanusGraphTest a = new JanusGraphTest();
        Object result = a.graph.executeGremlinScript("g.V().has('__state', 'ACTIVE').groupCount().by('__typeName').toList() ",
        false);
        System.out.println(result);
    }

    private final AtlasGraph graph;

    public JanusGraphTest() {
        this.graph = new AtlasJanusGraph(getBulkLoadingGraphInstance());
    }

    public static JanusGraph getBulkLoadingGraphInstance() {
        try {
            Configuration cfg = AtlasJanusGraphDatabase.getConfiguration();
            cfg.setProperty("storage.batch-loading", true);
            return JanusGraphFactory.open(cfg);
        } catch (IllegalArgumentException ex) {
        } catch (AtlasException ex) {
        }
        return null;
    }
}