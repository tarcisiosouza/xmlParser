package de.l3s.souza.xmlparser;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.DocumentBuilder;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

public class ReadXMLFileSolrFormat {

	private static String index;
	private static String type;                 
	private static String id;
	private static TransportClient client;
	private static Map<String, Object> data = new HashMap<String, Object>();

	
  public static void main(String argv[]) throws ParserConfigurationException, SAXException, IOException {

       
	  index  = "souza_livingknowledge_2";
      type   = "capture";
      client = getTransportClient("master02.ib", 9305);
      walk ("/home/souza/temporalia/im1c8.internetmemory.org/solr");
     
}
  
  public static void walk(String path) throws IOException, ParserConfigurationException, SAXException {

		File root = new File(path);
		File[] list = root.listFiles();

		if (list == null)
			return;
		
		for (File f : list) {
			if (f.isDirectory()) {
				walk(f.getAbsolutePath());
//				System.out.println("Dir:" + f.getAbsoluteFile());
			} else {
				
				if (!f.getAbsoluteFile().toString().contains("solr.xml"))
					continue;
				 File fXmlFile = new File(f.getAbsoluteFile().toString());
			        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			        DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			        Document doc = dBuilder.parse(fXmlFile);
			        
			        doc.getDocumentElement().normalize();

			        //    System.out.println("Root element :" + doc.getDocumentElement().getNodeName());

			            NodeList docs = doc.getElementsByTagName("field");
			            int docNumber = 0;
			            for (int i = 0; i < docs.getLength(); i++) {
			            	
			            	String node = docs.item(i).getAttributes().getNamedItem("name").getTextContent();
			            	
			            	switch (node)
			            	{
			            	case "url":data.put("url", docs.item(i).getTextContent());
			            	docNumber++;
			            	break;
			            	case "title":data.put("title", docs.item(i).getTextContent());
			            	docNumber++;
			            	break;
			            	case "sourcerss":data.put("sourcess", docs.item(i).getTextContent());
			            	docNumber++;
			            	break;
			            	case "id": data.put("id", docs.item(i).getTextContent());
			            	docNumber++;
			            	break;
			            	case "host":data.put("host", docs.item(i).getTextContent());
			            	docNumber++;
			            	break;
			            	case "date":data.put("date", docs.item(i).getTextContent());
			            	docNumber++;
			            	break;
			            	case "content":data.put("content", docs.item(i).getTextContent());
			            	docNumber++;
			            	break;
			            	}
			               // System.out.println(i+" " + docs.item(i).getTextContent());
			                
			                if (docNumber==7)
			                {
			                	docNumber=0;
			                	IndexResponse result = doIndex(client, index, type, id, data);
			                	System.out.println(((result.status().CREATED != null) ? "created" : "updated") + " document " + result.getId() );
			                	data.clear();
			                }
			                
			            }
				}
			
			
			}
		
		client.close();
		}
  public static TransportClient getTransportClient(String host, int port) throws UnknownHostException {

	  Settings settings = Settings.builder()
             
              .put("cluster.name", "kbs-esfive").build();
 	TransportClient client = new PreBuiltTransportClient(settings)
    //.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(hostname), port))
    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
 
 	return client;
 }
  
  public static IndexResponse doIndex(TransportClient client, String index, String type, String id, Map<String, Object> data) {

      return client.prepareIndex(index, type, id)
              .setSource(data)
              .execute()
              .actionGet();
  }
}
