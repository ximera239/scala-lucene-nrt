package io.github.ximera239.nrt

import java.io.File
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.Document
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig, Term, TrackingIndexWriter}
import org.apache.lucene.search._
import org.apache.lucene.store.{Directory, NIOFSDirectory, RAMDirectory}

import scala.sys.ShutdownHookThread

/**
  * Created by ximera239 on 27.04.17.
  */
class NRTSearcher(directory: Directory) {
  private val config = new IndexWriterConfig(new StandardAnalyzer()).setUseCompoundFile(true)
  private val writer = new IndexWriter(directory, config)
  protected val trackingWriter: TrackingIndexWriter = new TrackingIndexWriter(writer)
  protected val indexSearcherReferenceManager: SearcherManager =
    new SearcherManager(writer, true, true, null)
  protected val token: AtomicLong = new AtomicLong()
  private val isClosed = new AtomicBoolean(false)

  protected val indexSearcherReopenThread: ControlledRealTimeReopenThread[IndexSearcher] =
    new ControlledRealTimeReopenThread(trackingWriter,
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
    if (!isClosed.get) {
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
  case class SearchResult(documents: List[Document], totalHits: Int)

  def inFile(file: File) = new NRTSearcher(new NIOFSDirectory(file.toPath)) with Operations
  def inMem() = new NRTSearcher(new RAMDirectory()) with Operations

  trait Operations {
    protected def token: AtomicLong
    protected def trackingWriter: TrackingIndexWriter
    protected def indexSearcherReopenThread: ControlledRealTimeReopenThread[IndexSearcher]
    protected def indexSearcherReferenceManager: SearcherManager

    def addDocument(d: Document): Unit = {
      token.set(trackingWriter.addDocument(d))
    }

    def updateDocument(term: Term, d: Document): Unit = {
      token.set(trackingWriter.updateDocument(term, d))
    }

    def deleteDocuments(terms: Term*): Unit = {
      token.set(trackingWriter.deleteDocuments(terms: _*))
    }

    def deleteDocuments(query: Query): Unit = {
      token.set(trackingWriter.deleteDocuments(query))
    }

    def search(query: Query, n: Int): SearchResult = {
      indexSearcherReopenThread.waitForGeneration(token.get())
      implicit val s = indexSearcherReferenceManager.acquire()
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


