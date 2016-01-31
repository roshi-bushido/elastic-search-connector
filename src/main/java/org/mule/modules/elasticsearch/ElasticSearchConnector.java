package org.mule.modules.elasticsearch;

import java.io.IOException;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.json.JSONArray;
import org.json.JSONObject;
import org.mule.api.annotations.Config;
import org.mule.api.annotations.Connector;
import org.mule.api.annotations.Processor;
import org.mule.api.annotations.param.Optional;
import org.mule.modules.elasticsearch.config.ConnectorConfig;
import org.mule.modules.elasticsearch.config.DefaultResponseHandler;
import org.springframework.util.StringUtils;

@Connector(name="elastic-search", friendlyName="Elastic Search")
public class ElasticSearchConnector {

    @Config
    ConnectorConfig config;

    /**
     * List Elastic Search Indexes
     *
     * {@sample.xml ../../../doc/elastic-search-connector.xml.sample elastic-search:list-index}
     *
     * @return A JSonArray with all the Indexes
     * @throws IOException 
     * @throws ClientProtocolException 
     */
    @Processor
    public JSONArray listIndexes() throws ClientProtocolException, IOException {
        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpGet request = new HttpGet(this.config.getUrlFor("_cat/indices"));
        request.addHeader("Content-Type", "application/json");
        String body = httpClient.execute(request, new DefaultResponseHandler());
        return new JSONArray(body);
    }

    /**
     * List Elastic Search Indexes
     *
     * {@sample.xml ../../../doc/elastic-search-connector.xml.sample elastic-search:delete-index}
     *
     * @return A JSonArray with all the Indexes
     * @throws IOException 
     * @throws ClientProtocolException 
     */
    @Processor
    public Boolean deleteIndex(String indexName) throws ClientProtocolException, IOException {
    	if (!StringUtils.isEmpty(indexName)) {
    		try { 
                CloseableHttpClient httpClient = HttpClients.createDefault();
                HttpDelete request = new HttpDelete(this.config.getUrlFor(indexName));
                request.addHeader("Content-Type", "application/json");
                httpClient.execute(request, new DefaultResponseHandler());
                return true;
    		} catch (Exception e) {
    	        return false;
    		}
    	}
        return false;
    }    
    
    /**
     * Query Elastic Search Index
     *
     * {@sample.xml ../../../doc/elastic-search-connector.xml.sample elastic-search:query-index}
     *
     * @return A JSonArray with all the events
     * @throws IOException 
     * @throws ClientProtocolException 
     */
    @Processor
    public JSONObject queryIndex(@Optional String indexName, String jsonQuery) throws ClientProtocolException, IOException {
    	String searchUri = "_search";
    	if (!StringUtils.isEmpty(indexName)) {
    		searchUri = indexName + "/" + searchUri;
    	} 

    	CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpPost request = new HttpPost(this.config.getUrlFor(searchUri));
        request.addHeader("Content-Type", "application/json");
        request.setEntity(new ByteArrayEntity(jsonQuery.getBytes()));
        String body = httpClient.execute(request, new DefaultResponseHandler());
        return new JSONObject(body);
    }
    
    public ConnectorConfig getConfig() {
        return config;
    }

    public void setConfig(ConnectorConfig config) {
        this.config = config;
    }

}