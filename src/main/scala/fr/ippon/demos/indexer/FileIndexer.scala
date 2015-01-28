package fr.ippon.demos.indexer
/**
 * @author nguibert
 */
class FileIndexer (filepath: String, url: String) {

    import java.nio.file._
    import java.io.IOException
    import org.apache.commons.codec.binary.Base64
    import org.apache.http._
    import org.apache.http.client._
    import org.apache.http.client.methods.HttpPost
    import org.apache.http.entity.StringEntity
    import org.apache.http.impl.client._
    import java.util.ArrayList
    import org.apache.http.client.entity.UrlEncodedFormEntity
    
    /**
     * injecte le fichier dans la base documentaire
     */
    def upsert(fileobj : Path) {
      try {
        if (! isAlreadyIndexed(fileobj)) {
          val serialized : String = jsonSerializer(fileobj)
          val httpResponse = postRequest(serialized, url)
          
          checkDocumentIndexation(httpResponse.getStatusLine.getStatusCode, 
                                  new BasicResponseHandler().handleResponse(httpResponse))
        }
      } catch {
        case ioe : IOException => ioe.printStackTrace 
      }
    }
    
    /**
     * Interroge elasticsearch pour verifier si un document a déjà été indexé.
     */
    def isAlreadyIndexed(fileobj : Path) : Boolean = {
      val searchUrl = url + "_search"
      val filename = fileobj.getFileName.toString
      val jsonData = "{\"fields\" : [\"name\"]," +
                      "\"query\": {" +
                        "\"match_phrase\": {" +
                          "\"file.name\": \"" + filename + "\"" +
                        "}" +
                      "}" +
                     "}"      

      val resp = postRequest(jsonData, searchUrl)      
      val status = resp.getStatusLine.getStatusCode
      if (status == HttpStatus.SC_OK) { 
        val respBody = new BasicResponseHandler().handleResponse(resp)
        println("OK - " + respBody)
        return (getHitsCount(respBody) > 0)
      } else 
        return false
    }
    
    /**
     * Parse une response elasticseacrh au format JSON et compte le nombre de résultats.
     */
    def getHitsCount(jsonData : String) : Long = {
      import org.json.simple._
      import org.json.simple.parser._ 
      
      val parser : JSONParser = new JSONParser
      val root : JSONObject = parser.parse(jsonData).asInstanceOf[JSONObject]
      val hits = root.get("hits")
      hits match {
        case None => return 0
        case obj : JSONObject => {
          val total = obj.get("total")
              total match {            
                case nbr : Long => return nbr
                case _ => return 0
          }
        }
      }
      return 0
    }
    
    /**
     * helper HTTP POST
     */
    def postRequest(jsonData : String, serverUrl : String) : HttpResponse = {
      // TODO utiliser log4j a la place.
      println("POST " + serverUrl)
      
      val httpClient = new DefaultHttpClient;
      val post = new HttpPost(serverUrl)
      post.setHeader("Content-type", "application/json")
      post.setEntity(new StringEntity(jsonData))
      val response = httpClient.execute(post)     
      httpClient.close();
      
      return response
    }
    
    /**
     * Vérifie le code retour d'elasticsearch pour une requête d'indexation de documents
     */
    def checkDocumentIndexation(statusCode : Int, body : String) : Boolean = {
      if (statusCode == HttpStatus.SC_CREATED) {
          import org.json.simple._
          import org.json.simple.parser._ 
      
          try {
            val parser : JSONParser = new JSONParser
            val root : JSONObject = parser.parse(body).asInstanceOf[JSONObject]
            
            println("Index cree avec succes")
            val created = root.get("created")
            created match {
              case None => return false
              case value : Boolean => return value
            }
          } catch {
            case ex : Exception => {
              ex.printStackTrace()
              return false
            }
          }
      } else {
        println("Index non cree - HTTP " + statusCode)
        println("Reponse de elasticsearch :" + body)
        return false
      }
    }
    
    /** Serialisation du fichier au format json */
    def jsonSerializer(fileobj : Path) : String = {
      // Encodage du fichier en base 64 
      val byteArray = Files.readAllBytes(fileobj)
      val b64Encoded = Base64.encodeBase64String(byteArray)
      
      // Injection du contenu encode en base 64 dans un template JSON
      val absolutepath = fileobj.toAbsolutePath().toString()
      return "{\"file\":{\"_indexed_chars\" : -1, \"_name\" : \"" + absolutepath + "\", \"_content\" : \"" + b64Encoded + "\"}}"
    }

    /**
     * Indexe un fs dans elasticsearch pour permettre de faire des recherches avancées sur le contenu des fichiers.
     */
    def index {
      index(Paths.get(filepath));
    }    
    
    /**
     * fonction recursive d'indexation.
     */
    def index(fp :Path) {
      if (Files.exists(fp)) {
        if (Files.isDirectory(fp)) { 
          var it = Files.newDirectoryStream(fp).iterator();
          while (it.hasNext()) {
            index(it.next())
          }
        } else {
          if (Files.isRegularFile(fp) && !Files.isHidden(fp) && Files.isReadable(fp)) {
            upsert(fp)
          }
        }
      } else {
        println(fp.toString + " does not exist !")
      }
    }
}