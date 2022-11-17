package io.lenses.streamreactor.connect.aws.s3.formats

import io.lenses.streamreactor.connect.aws.s3.model.SchemaAndValueSourceData
import io.lenses.streamreactor.connect.aws.s3.model.location.RemoteS3PathLocation

import java.io.InputStream
import scala.io.Source
import scala.util.Try

import scala.jdk.CollectionConverters.MapHasAsJava
import org.apache.kafka.connect.json.JsonConverter

class JsonFormatStreamReader(inputStreamFn: () => InputStream, bucketAndPath: RemoteS3PathLocation)
  extends S3FormatStreamReader[SchemaAndValueSourceData] {

  private val inputStream: InputStream = inputStreamFn()
  private val source = Source.fromInputStream(inputStream, "UTF-8")
  protected val sourceLines = source.getLines()
  protected var lineNumber: Long = -1

  private val jsonConverter = new JsonConverter

  jsonConverter.configure(
    Map("schemas.enable" -> false).asJava,
    false,
  )

  override def close(): Unit = {
    val _ = Try(source.close())
  }

  override def hasNext: Boolean = sourceLines.hasNext


  override def next(): SchemaAndValueSourceData = {
    lineNumber += 1
    if (!sourceLines.hasNext) {
      throw FormatWriterException(
        "Invalid state reached: the file content has been consumed, no further calls to next() are possible.",
      )
    }
    val genericRecordData = sourceLines.next();
    val genericRecord = if (genericRecordData == null) null else genericRecordData.getBytes();
    val schemaAndValue = jsonConverter.toConnectData("", genericRecord)
    SchemaAndValueSourceData(schemaAndValue, lineNumber)
  }

  override def getBucketAndPath: RemoteS3PathLocation = bucketAndPath

  override def getLineNumber: Long = lineNumber
}