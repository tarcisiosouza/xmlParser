package de.l3s.souza.xmlparser;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.DocumentBuilder;

import org.apache.lucene.util.packed.PackedLongValues.Iterator;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
//import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.runner.RunWith;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import org.w3c.dom.Node;
import org.w3c.dom.Element;

import com.fasterxml.jackson.databind.ObjectMapper;

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
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class ReadXMLFile {

	private static String index;
	private static String type;
	private static String id;
	private static Client client;
	private static Map<String, String> data = new HashMap<String, String>();
	private static BulkRequestBuilder bulkBuilder;

/*	
	public static void main(String argv[]) throws ParserConfigurationException,
			SAXException, IOException {

		index = "souza_livingknowledge";
		type = "capture";
	//	client = getTransportClient("master02.ib", 9350);
		client = getTransportClient("master02.ib", 9305);
		// 0 walk ("/Volumes/Priest/Temporalia");
		walk("/home/souza/temporalia/im1c8.internetmemory.org/lk");
		
		client.close();

	}
*/
	private static Pattern TAG_REGEX;

	private static List<String> getTagValues(final String str) {
		final List<String> tagValues = new ArrayList<String>();
		final Matcher matcher = TAG_REGEX.matcher(str);
		while (matcher.find()) {
			tagValues.add(matcher.group(1));
		}
		return tagValues;
	}

	public static void walk(String path) throws IOException,
			ParserConfigurationException, SAXException {

		File root = new File(path);
		File[] list = root.listFiles();

		if (list == null)
			return;

		HashMap<String, String> tempExp = new HashMap<String, String>();
		String docID = null;
		String host = null;
		String date = null;
		String url = null;
		String title = null;
		String text = "";
		String pureText = "";
		String temporalExp = "";
		String tempPlusOriginal = "";
		int bulkBuilderLength = 0;
		bulkBuilder = client.prepareBulk();
		for (File f : list) {
			if (f.isDirectory()) {
				walk(f.getAbsolutePath());
				// System.out.println("Dir:" + f.getAbsoluteFile());
			} else {

				FileReader fr = new FileReader(f);
				BufferedReader br = new BufferedReader(fr);
				String line;
				while ((line = br.readLine()) != null) {

					if (line.contains("doc id")) {

						try {
							TAG_REGEX = Pattern.compile("<doc id=(.+?)>");
							final Pattern pattern = Pattern
									.compile("<doc id=(.+?)>");
							final Matcher matcher = pattern.matcher(line);
							matcher.find();
							docID = matcher.group(1);
						} catch (Exception e) {
							docID = "";
						}
					} else

					if (line.contains("<tag name=\"host\">")) {
					try {	
						final Pattern pattern = Pattern
								.compile("<tag name=\"host\">(.+?)</tag>");
						final Matcher matcher = pattern.matcher(line);
						matcher.find();
						host = matcher.group(1);
					} catch (Exception e)
					{
						host = "";
					}
					
					} else

					if (line.contains("<tag name=\"url\">")) {
						
					try {	
						final Pattern pattern = Pattern
								.compile("<tag name=\"url\">(.+?)</tag>");
						final Matcher matcher = pattern.matcher(line);
						matcher.find();
						url = matcher.group(1);
					} catch (Exception e)
					{
						url = "";
					}
					} else

					if (line.contains("<tag name=\"date\">")) {
					try {	
						final Pattern pattern = Pattern
								.compile("<tag name=\"date\">(.+?)</tag>");
						final Matcher matcher = pattern.matcher(line);
						matcher.find();
						date = matcher.group(1);
					} catch (Exception e)
					{
						date = "";
					}
					} else if (line.contains("<tag name=\"title\">")) {
					try {	
						final Pattern pattern = Pattern
								.compile("<tag name=\"title\">(.+?)</tag>");
						final Matcher matcher = pattern.matcher(line);
						matcher.find();
						title = matcher.group(1);
					} catch (Exception e)
					{
						title = "";
					}
					} else if (!line.contains("<meta-info>")
							&& !line.contains("<tag")
							&& !line.contains("</meta-info")) {
						text = text + line;
					}

					if (line.contains("</doc")) {

						pureText = text.replaceAll("<[^>]+>", "");

						TAG_REGEX = Pattern.compile("<T(.+?)</T>");
						List<String> test = getTagValues(text);

						java.util.Iterator<String> iter = test.iterator();

						while (iter.hasNext()) {
							String current = iter.next();
							Pattern pattern = Pattern.compile("val=\"(.+?)\">");
							Matcher matcher = pattern.matcher(current);
							matcher.find();
							String temp = matcher.group(1);

							int size = current.length();
							int position = 0;
							int i = 0;
							String original = "";
							while (true) {
								if (current.charAt(i) != '>') {
									position++;
									i++;
									continue;
								} else {

									break;
								}
							}

							original = current.substring(position + 1, size);

							tempExp.put(original, temp);
							// tempPlusOriginal = tempPlusOriginal + original +
							// "," + temp + "\n";
						}

						for (Entry<String, String> s : tempExp.entrySet()) {
							tempPlusOriginal += s.getKey() + "," + s.getValue()
									+ "\n";
						}

						data.put("url", url);
						data.put("id", docID);
						data.put("text", pureText);
						
						
						data.put("host", host);
						data.put("date", date);
						data.put("title", title);
						data.put("temp", tempPlusOriginal);
						
						String json = new ObjectMapper().writeValueAsString(data);						   
						bulkBuilder.add(client.prepareIndex(index, type, id).setSource(json));
						bulkBuilderLength++;
						
						 if(bulkBuilderLength % 1000 == 0){
						      System.out.println("##### " + bulkBuilderLength + " data indexed.");
						      BulkResponse bulkRes = bulkBuilder.execute().actionGet();
						      if(bulkRes.hasFailures()){
						    	  System.out.println("##### Bulk Request failure with "
						    	  		+ "error: " + bulkRes.buildFailureMessage());
						      }
						      bulkBuilder = client.prepareBulk();
						   }
						/*
						 * IndexResponse result = doIndex(client, index, type,
						 * id, data); System.out.println((result.isCreated() ?
						 * "created" : "updated") + " document " +
						 * result.getId()); data.clear();
						 */
						tempPlusOriginal = docID = host = date = url = title = text = pureText = "";
						tempExp.clear();
					}

				}

			}
		}

	}
	
	public static Client getTransportClient(String host, int port)
			throws UnknownHostException {

		Settings settings = Settings.builder()
				.put("client.transport.sniff", true)
				// .put("shield.user", "souza:pri2006")
			//	.put("client.transport.ping_timeout", "30s")
				.put("cluster.name", "kbs-esfive").build();
		
		
	//	@SuppressWarnings("resource")
	/*	
		TransportClient client = new PreBuiltTransportClient(settings)
				.addTransportAddress(
						new InetSocketTransportAddress(InetAddress
								.getByName(host), port))
				.addTransportAddress(
						new InetSocketTransportAddress(InetAddress
								.getByName(host), port));
*/
		return client;
	}

	public static IndexResponse doIndex(Client client, String index,
			String type, String id, Map<String, Object> data) {

		return client.prepareIndex(index, type, id).setSource(data).execute()
				.actionGet();
	}
}
