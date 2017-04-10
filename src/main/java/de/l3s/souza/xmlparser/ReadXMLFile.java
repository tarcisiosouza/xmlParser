package de.l3s.souza.xmlparser;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.DocumentBuilder;

import org.apache.lucene.util.packed.PackedLongValues.Iterator;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import org.w3c.dom.Node;
import org.w3c.dom.Element;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ReadXMLFile {

	private static String index;
	private static String type;                 
	private static String id;
	private static Client client;
	private static Map<String, Object> data = new HashMap<String, Object>();

	
  public static void main(String argv[]) throws ParserConfigurationException, SAXException, IOException {

       
	  index  = "souza_livingknowledge";
      type   = "capture";
      client = getTransportClient("localhost", 9200);
//    0  walk ("/Volumes/Priest/Temporalia");
      walk ("/Users/tarcisio/Downloads/test");
     
}
  private static Pattern TAG_REGEX ;

  private static List<String> getTagValues(final String str) {
      final List<String> tagValues = new ArrayList<String>();
      final Matcher matcher = TAG_REGEX.matcher(str);
      while (matcher.find()) {
          tagValues.add(matcher.group(1));
      }
      return tagValues;
  }
  public static void walk(String path) throws IOException, ParserConfigurationException, SAXException {

		File root = new File(path);
		File[] list = root.listFiles();

		if (list == null)
			return;
		
		String docID;
		String host;
		String date;
		String url;
		String title;
		String text = "";
		String pureText = "";
		String temporalExp="";
		
		for (File f : list) {
			if (f.isDirectory()) {
				walk(f.getAbsolutePath());
//				System.out.println("Dir:" + f.getAbsoluteFile());
			} else {
				
				FileReader fr =  new FileReader (f);
				BufferedReader br = new BufferedReader (fr);
				String line;
				while ((line=br.readLine())!=null)
				{
					
					if  (line.contains("doc id"))
					{
						TAG_REGEX= Pattern.compile("<doc id=(.+?)>");
						final Pattern pattern = Pattern.compile("<doc id=(.+?)>");
						final Matcher matcher = pattern.matcher(line);
						matcher.find();
						docID = matcher.group(1);
					}
					else
					
					if (line.contains("<tag name=\"host\">"))
					{
						final Pattern pattern = Pattern.compile("<tag name=\"host\">(.+?)</tag>");
						final Matcher matcher = pattern.matcher(line);
						matcher.find();
						host = matcher.group(1);
						
					}
					else
						
					if (line.contains("<tag name=\"url\">"))
					{
						final Pattern pattern = Pattern.compile("<tag name=\"url\">(.+?)</tag>");
						final Matcher matcher = pattern.matcher(line);
						matcher.find();
						url = matcher.group(1);
						
					}
					else
						
					if (line.contains("<tag name=\"date\">"))
					{
						final Pattern pattern = Pattern.compile("<tag name=\"date\">(.+?)</tag>");
						final Matcher matcher = pattern.matcher(line);
						matcher.find();
						date = matcher.group(1);
						
					}
					else
					if (line.contains("<tag name=\"title\">"))
					{
						final Pattern pattern = Pattern.compile("<tag name=\"title\">(.+?)</tag>");
						final Matcher matcher = pattern.matcher(line);
						matcher.find();
						title = matcher.group(1);
						
					}
					else
						if (!line.contains("<meta-info>") && !line.contains("<tag") && !line.contains("</meta-info"))
						{
							text = text + line;
						}
					
					
					if (line.contains("</doc"))
					{
						
						 pureText = text.replaceAll("<[^>]+>", "");
						 
						 //<T val="201101">January</T>
						 TAG_REGEX= Pattern.compile("<T(.+?)</T>");
						 List<String> test = getTagValues (text);
						 
						 java.util.Iterator<String> iter = test.iterator();
						 
						 //val="20110602">Thursday evening
						 while (iter.hasNext())
						 {
							 String current = iter.next();
							  Pattern pattern = Pattern.compile("val=\"(.+?)\">");
								 Matcher matcher = pattern.matcher(current);
								matcher.find();
								String temp = matcher.group(1);
								
							int size = current.length();
							int position=0;
							int i = 0;
							String original="";
							while (true)
							{
								if (current.charAt(i)!='>')
								{
									position += 1;
									i++;
									continue;
								}
								else
									break;
								
							}
							
							original = current.substring(position+1, size);
							 temporalExp += iter.next();
						 }
						 docID = host = date = url = title = text = "";
					}
				}
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
			                	System.out.println((result.isCreated() ? "created" : "updated") + " document " + result.getId() );
			                	data.clear();
			                }
			            }
				}
			
			
			}
		}
  public static Client getTransportClient(String host, int port) throws UnknownHostException {

 	 Settings settings = Settings.settingsBuilder()
              .put("client.transport.sniff", true)
             // .put("shield.user", "souza:pri2006")
              .put("sniffOnConnectionFault",true)

              .put("cluster.name", "nextsearch").build();
 	TransportClient client = TransportClient.builder().settings(settings).build()
             .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port))
     .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
 	
 	return client;
 }
  
  public static IndexResponse doIndex(Client client, String index, String type, String id, Map<String, Object> data) {

      return client.prepareIndex(index, type, id)
              .setSource(data)
              .execute()
              .actionGet();
  }
}
