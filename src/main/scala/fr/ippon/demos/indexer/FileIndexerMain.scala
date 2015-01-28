package fr.ippon.demos.indexer

object FileIndexerMain {
  def main(args: Array[String]) {
    if (args.length >= 2)
      new FileIndexer(args(0), args(1)).index
    else {
      println("Ce programme prend deux parametres :")
      println(" 1 . Path de l'arborescence a indexer")
      println(" 2 . URL d'indexation (ElasticSearch)")
    }
  }
}