package com;

import com.google.common.hash.Hashing;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

class ElasticConnector {
    PreBuiltTransportClient client = null;

    void initialize() {
        try {
            Settings settings = Settings.builder()
                    .put("cluster.name","docker-cluster")
                    .build();
            PreBuiltTransportClient cli = new PreBuiltTransportClient(settings);
            cli.addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"),9300));
            client = cli;
        }
        catch (Exception e) {
            System.out.println("something wrong");
        }
    }

    public boolean IndexExists(String index) {
        return client.admin().indices().exists(new IndicesExistsRequest(index)).actionGet().isExists();
    }

    void AddIndex(String index) {
        try {
            if (IndexExists(index)) {
                return;
            } else {
                client.admin().indices().create(new CreateIndexRequest(index)).actionGet();
            }
        } catch (Exception e) {
            System.out.println("can not create index " + e);
        }
    }

    void AddMapping(String index) {
        try {
            XContentBuilder mapping = jsonBuilder()
                    .startObject()
                    .startObject("properties")
                    .startObject("Title")
                    .field("type", "keyword")
                    .endObject()
                    .startObject("Text")
                    .field("type", "text")
                    .endObject()
                    .endObject()
                    .endObject();

            client.admin().indices()
                    .preparePutMapping(index)    // add mapping definition for a type into indices
                    .setSource(mapping)
                    .setType("_doc")
                    .execute().actionGet();
        } catch (Exception e) {
            System.out.println("can not set mapping " + e);
        }
    }

    String getHash(String json) {
        JsonElement jelement = new JsonParser().parse(json);
        JsonObject jobject = jelement.getAsJsonObject();
        System.out.println(jobject.get("Text").toString());
        //System.out.println(getSHA(jobject.get("Text").toString()));

        String sha256hex = Hashing.sha256()
                .hashString(jobject.get("Text").toString(), StandardCharsets.UTF_8)
                .toString();
        return sha256hex;
    }

    void AddDoc(String json) {
        IndexRequest request = new IndexRequest("news");
        try {
            request.id(getHash(json));    // document id is a hash of text
        } catch (Exception e) {
            System.out.println("error while getting hash");
        }
        request.source(json, XContentType.JSON);
        client.index(request);
    }

    void CloseConnection()
    {
        client.close();
    }

    void getSomeDataAll() {
        QueryBuilder query = QueryBuilders.matchAllQuery();
        SearchResponse response = client.prepareSearch("*").setQuery(query).get();
        System.out.println(response.getHits().getTotalHits());
    }

    void getSomeData() {
        QueryBuilder query = QueryBuilders.termQuery("Title", "В КЦ «Октябрь» пройдет показ фильма «Подольские курсанты»");
        SearchResponse response = client.prepareSearch("news").setQuery(query).get();
        System.out.println(response.getHits().getTotalHits());
        System.out.println(response);
    }

    void getSomeDataList() {
        QueryBuilder query = QueryBuilders.termQuery("Title", "В КЦ «Октябрь» пройдет показ фильма «Подольские курсанты»");
        SearchResponse response = client.prepareSearch("news").setQuery(query).get();

        Iterator<SearchHit> sHits = response.getHits().iterator();
        List<String> results = new ArrayList<String>(20); //initial size of array
        while (sHits.hasNext()) {
            results.add(sHits.next().getSourceAsString());
            System.out.println(results);
        }
        System.out.println(response.getHits().getTotalHits());
    }

    SearchResponse SearchNews(String index, String field, String phrase)
    {
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchQuery(field, phrase));
        searchRequest.source(sourceBuilder);
        SearchResponse resp = client.search(searchRequest).actionGet();
        return resp;
    }
}
