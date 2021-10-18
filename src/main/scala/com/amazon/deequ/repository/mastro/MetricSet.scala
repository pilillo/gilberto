package com.amazon.deequ.repository.mastro

import com.amazon.deequ.analyzers.runners.AnalyzerContext
import com.amazon.deequ.analyzers.{Analyzer, State}
import com.amazon.deequ.metrics.{Distribution, Metric}
import com.amazon.deequ.repository._
import com.google.gson._
import com.google.gson.reflect.TypeToken

import java.lang.reflect.Type
import java.util.{ArrayList => JArrayList, HashMap => JHashMap, List => JList, Map => JMap}
import scala.collection.JavaConverters._
import scala.collection._

case class MetricSet(name : String,
                     version : String,
                     description : String,
                     labels : Map[String,String],
                     metrics : List[AnalysisResult])

object MastroFields {
  val NAME = "name"
  val VERSION = "version"
  val DESCRIPTION = "description"
  val LABELS = "labels"
  val METRICS = "metrics"
}

private[mastro] object MetricSetSerializer extends JsonSerializer[MetricSet] {

  override def serialize(metricSet: MetricSet,
                         t: Type,
                         context: JsonSerializationContext): JsonElement = {

    val result = new JsonObject()

    result.add(MastroFields.NAME, context.serialize(metricSet.name, classOf[String]))
    result.add(MastroFields.VERSION, context.serialize(metricSet.version, classOf[String]))
    result.add(MastroFields.DESCRIPTION, context.serialize(metricSet.description, classOf[String]))
    result.add(MastroFields.LABELS,
      context.serialize(metricSet.labels.asJava, new TypeToken[JMap[String,String]]() {}.getType)
    )
    result.add(MastroFields.METRICS,
      context.serialize(metricSet.metrics.asJava, new TypeToken[JList[String]]() {}.getType)
    )

    result
  }
}

private[mastro] object MetricSetDeserializer extends JsonDeserializer[MetricSet] {

  override def deserialize(jsonElement: JsonElement, t: Type,
                           context: JsonDeserializationContext): MetricSet = {

    val jsonObject = jsonElement.getAsJsonObject

    val name : String = context.deserialize(jsonObject.get(MastroFields.NAME), classOf[String])
    val version : String = context.deserialize(jsonObject.get(MastroFields.VERSION), classOf[String])
    val description : String = context.deserialize(jsonObject.get(MastroFields.DESCRIPTION), classOf[String])

    //val labels : Map[String, String] = context.deserialize(jsonObject.get(MastroFields.LABELS), classOf[Map[String,String]])
    val metrics = context.deserialize(
      jsonObject.get(MastroFields.METRICS),
      new TypeToken[JList[AnalysisResult]]() {}.getType
      //classOf[List[AnalysisResult]]
    ).asInstanceOf[JList[AnalysisResult]].asScala.toList

    val labels = context.deserialize(
      jsonObject.get(MastroFields.LABELS),
      new TypeToken[JHashMap[String, String]]() {}.getType
    ).asInstanceOf[JHashMap[String, String]].asScala.toMap

    MetricSet(
      name,
      version,
      description,
      labels,
      metrics
    )
  }
}


object MastroSerde {

  def serialize(metricSet: MetricSet): String = {
    val gson = new GsonBuilder()
      .registerTypeAdapter(classOf[MetricSet], MetricSetSerializer)
      .registerTypeAdapter(classOf[ResultKey], ResultKeySerializer)
      .registerTypeAdapter(classOf[AnalysisResult], AnalysisResultSerializer)
      .registerTypeAdapter(classOf[AnalyzerContext], AnalyzerContextSerializer)
      .registerTypeAdapter(classOf[Analyzer[State[_], Metric[_]]],
        AnalyzerSerializer)
      .registerTypeAdapter(classOf[Metric[_]], MetricSerializer)
      .registerTypeAdapter(classOf[Distribution], DistributionSerializer)
      .setPrettyPrinting()
      .create

    gson.toJson(metricSet, new TypeToken[MetricSet]() {}.getType)
  }

  def deserialize(metricSet : String): MetricSet = {
    val gson = new GsonBuilder()
      .registerTypeAdapter(classOf[MetricSet], MetricSetDeserializer)
      .registerTypeAdapter(classOf[ResultKey], ResultKeyDeserializer)
      .registerTypeAdapter(classOf[AnalysisResult], AnalysisResultDeserializer)
      .registerTypeAdapter(classOf[AnalyzerContext], AnalyzerContextDeserializer)
      .registerTypeAdapter(classOf[Analyzer[State[_], Metric[_]]], AnalyzerDeserializer)
      .registerTypeAdapter(classOf[Metric[_]], MetricDeserializer)
      .registerTypeAdapter(classOf[Distribution], DistributionDeserializer)
      .create

    gson.fromJson(metricSet,
      new TypeToken[MetricSet]() {}.getType)
      .asInstanceOf[MetricSet]
  }

  def deserializeMultiple(metricSets : String) : Seq[MetricSet] = {
    val gson = new GsonBuilder()
      .registerTypeAdapter(classOf[MetricSet], MetricSetDeserializer)
      .registerTypeAdapter(classOf[ResultKey], ResultKeyDeserializer)
      .registerTypeAdapter(classOf[AnalysisResult], AnalysisResultDeserializer)
      .registerTypeAdapter(classOf[AnalyzerContext], AnalyzerContextDeserializer)
      .registerTypeAdapter(classOf[Analyzer[State[_], Metric[_]]], AnalyzerDeserializer)
      .registerTypeAdapter(classOf[Metric[_]], MetricDeserializer)
      .registerTypeAdapter(classOf[Distribution], DistributionDeserializer)
      .create

    gson.fromJson(metricSets,
      new TypeToken[JList[MetricSet]]() {}.getType)
      .asInstanceOf[JArrayList[MetricSet]].asScala
  }
}