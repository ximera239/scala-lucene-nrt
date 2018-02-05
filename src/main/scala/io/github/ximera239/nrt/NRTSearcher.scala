package io.github.ximera239.nrt

import java.io.File
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.Document
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig, Term}
import org.apache.lucene.search._
import org.apache.lucene.store.{Directory, NIOFSDirectory, RAMDirectory}

import scala.sys.ShutdownHookThread

/**
  * Created by ximera239 on 27.04.17.
  */
class NRTSearcher(directory: Directory) {
  private val config = new IndexWriterConfig(new StandardAnalyzer()).setUseCompoundFile(true)
  protected val writer = new IndexWriter(directory, config)
  protected val indexSearcherReferenceManager: SearcherManager =
    new SearcherManager(writer, true, true, null)
  protected val token: AtomicLong = new AtomicLong()
  private val isClosed = new AtomicBoolean(false)

  protected val indexSearcherReopenThread: ControlledRealTimeReopenThread[IndexSearcher] =
    new ControlledRealTimeReopenThread(writer,
      indexSearcherReferenceManager,
      60.00,
      0.01)
  indexSearcherReopenThread.start()

  ShutdownHookThread {
    close()
  }

  def commit(): Unit = {
    writer.commit()
  }

  def close(): Boolean = isClosed.synchronized {
    if (!isClosed.getAndSet(true)) {
      indexSearcherReopenThread.interrupt()
      indexSearcherReopenThread.close()

      // Close the indexWriter, commiting everithing that's pending
      if (writer.isOpen) {
        writer.commit()
        writer.close()
      }
      isClosed.set(true)
      true
    } else false
  }
}

object NRTSearcher {
  case class SearchResult(documents: List[Document], totalHits: Long)

  def inFile(file: File) = new NRTSearcher(new NIOFSDirectory(file.toPath)) with Operations
  def inMem() = new NRTSearcher(new RAMDirectory()) with Operations

  type SearchType = (Query, Int) => SearchResult
  type CountType = Query => Int

  trait SearchOperations {
    def search: SearchType
    def count: CountType
  }

  trait Operations {
    protected def token: AtomicLong
    protected def writer: IndexWriter
    protected def indexSearcherReopenThread: ControlledRealTimeReopenThread[IndexSearcher]
    protected def indexSearcherReferenceManager: SearcherManager

    def addDocument(d: Document): Unit = {
      token.set(writer.addDocument(d))
    }

    def updateDocument(term: Term, d: Document): Unit = {
      token.set(writer.updateDocument(term, d))
    }

    def deleteDocuments(terms: Term*): Unit = {
      token.set(writer.deleteDocuments(terms: _*))
    }

    def deleteDocuments(query: Query): Unit = {
      token.set(writer.deleteDocuments(query))
    }

    def search(query: Query, n: Int): SearchResult = {
      indexSearcherReopenThread.waitForGeneration(token.get())
      implicit val s: IndexSearcher = indexSearcherReferenceManager.acquire()
      try {
        s.search(query, n).toSearchResult
      } finally {
        indexSearcherReferenceManager.release(s)
      }
    }

    def count(query: Query): Int = {
      indexSearcherReopenThread.waitForGeneration(token.get())
      val s = indexSearcherReferenceManager.acquire()
      try {
        s.count(query)
      } finally {
        indexSearcherReferenceManager.release(s)
      }
    }

    def withStaticSearcher[T](f: (SearchOperations) => T): T = {
      indexSearcherReopenThread.waitForGeneration(token.get())
      implicit val s: IndexSearcher = indexSearcherReferenceManager.acquire()
      try {
        f(new SearchOperations {
          override def search: SearchType = (q, i) => s.search(q, i).toSearchResult
          override def count: CountType = (q) => s.count(q)
        })
      } finally {
        indexSearcherReferenceManager.release(s)
      }
    }
  }

  implicit class TopDocsWrapper(topDocs: TopDocs) {
    def toSearchResult(implicit indexSearcher: IndexSearcher): SearchResult = {
      SearchResult(
        Option(topDocs.scoreDocs).
          toList.
          flatMap(_.map(d => indexSearcher.doc(d.doc))),
        topDocs.totalHits)
    }
  }
}


