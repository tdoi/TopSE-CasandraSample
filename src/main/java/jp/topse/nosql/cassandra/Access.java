package jp.topse.nosql.cassandra;

import java.util.Map;

public class Access {
    private String target;
    private Map<String, String> params;
    private String referer;
    
    public Access(String target, Map<String, String> params, String referer) {
        this.target = target;
        this.params = params;
        this.referer = referer;
    }

    public String getTarget() {
        return target;
    }

    public Map<String, String> getParams() {
        return params;
    }

    public String getReferer() {
        return referer;
    }
}
