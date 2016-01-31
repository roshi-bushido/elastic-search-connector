package org.mule.modules.elasticsearch.config;

import org.apache.commons.lang.StringUtils;
import org.mule.api.annotations.components.Configuration;
import org.mule.api.annotations.Configurable;
import org.mule.api.annotations.param.Default;

@Configuration(friendlyName = "Elastic Search Configuration")
public class ConnectorConfig {

    @Configurable
    @Default("http://localhost:9200")
    private String url;
    
    public void setUrl(String url) {
		this.url = url;
	}
	public String getUrl() {
		return url;
	}

	public String getUrlFor(String path) {
    	if (!StringUtils.isEmpty(path)) {
    		if (path.startsWith("/")) {
    			return this.url + path;
    		} else {
    			return this.url + "/" + path;
    		}
    	} else {
    		return this.url;
    	}
    }
	
}