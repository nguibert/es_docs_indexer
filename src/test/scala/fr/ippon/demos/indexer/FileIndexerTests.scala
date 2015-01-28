
package fr.ippon.demos.indexer


import org.apache.http._
import org.apache.http.client._
import org.apache.http.client.methods._
import org.apache.http.impl.client._
import org.apache.http.entity.StringEntity
import org.json.simple._
import org.json.simple.parser._
import org.scalatest.FunSuite

/**
 * @author nguibert
 */
class FileIndexerTests extends FunSuite {

	val elastisearchUrl = "http://localhost:9200/filesystem/docs/"

	def retrieveIndexedDocId(response : String) : Option[String] = {
		try {
			try {
				val parser : JSONParser = new JSONParser
				val root : JSONObject = parser.parse(response).asInstanceOf[JSONObject]

				val id = root.get("_id")
				id match {
					case None => return None
  				case value : String => return Some(value)
				}
			} catch {
  			case ex : Exception => {
  				ex.printStackTrace()
  				return None
 				}
			}
		} catch {
			case ex : Exception => ex.printStackTrace();
		}
		return None
	}

	def deleteDoc(docId : String) : HttpResponse = {
		val httpClient = new DefaultHttpClient;
		val req = new HttpDelete(elastisearchUrl + docId)
		val resp = httpClient.execute(req)    
		httpClient.close();
    
    return resp
	}

	test("Put Document") {
    import java.nio.file._
    try {
      val testfilepath : Path = Paths.get(getClass.getResource("/test.txt").getPath)
      val fileindexer = new FileIndexer("/", elastisearchUrl)

      val serialized : String = fileindexer.jsonSerializer(testfilepath)
      val resp = fileindexer.postRequest(serialized, elastisearchUrl)
      val respStatus = resp.getStatusLine.getStatusCode
      val respBody = new BasicResponseHandler().handleResponse(resp)          

      assert(fileindexer.checkDocumentIndexation(respStatus, respBody))

      val indexedDocId = retrieveIndexedDocId(respBody)
      indexedDocId match {
        case None => fail
        case Some(id) => {
          assert(fileindexer.isAlreadyIndexed(testfilepath))
          deleteDoc(id)
        }
      }
    } catch {
      case ex : Exception => {
        ex.printStackTrace()
        fail(ex.getMessage)
      }
    }
	}

	test("Base 64 encoding") {
		import java.nio.file._
		try {
			val testfilepath : Path = Paths.get(getClass.getResource("/test.txt").getPath)

			val fileindexer = new FileIndexer("/", elastisearchUrl)
		  val json = fileindexer.jsonSerializer(testfilepath)
      
      val parser : JSONParser = new JSONParser
      val root : JSONObject = parser.parse(json).asInstanceOf[JSONObject]
      val file = root.get("file")
      file match {
        case None => fail
        case value : JSONObject => {
          val content = value.get("_content")
          content match {
            case None => fail
            case str : String => {
              import org.apache.commons.codec.binary.Base64
              
              val decoded = new String(Base64.decodeBase64(str))
              assert(decoded === "This is a test resource.")
            }
            case _ => fail
          }
        }
        case _ => fail
      }
		} catch {
  		case ex : Exception => {
  			ex.printStackTrace()
  			fail(ex.getMessage)
  		}
		}
	}

}