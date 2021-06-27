package com.fca.conceptgeneration
import java.text.SimpleDateFormat
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
//import com.esotericsoftware.kryo.Kryo

import scala.concurrent.ExecutionContext
import org.spark_project.guava.collect.ImmutableSet

case class Concept(extent: Set[String], intent: Set[String], isValidNeighbor: Boolean, var parent_index: Integer)

object ThreewayData extends Serializable {
  var generatedPositiveconceptIntent = Set[Set[String]]()
  var generatedNegativeconceptIntent = Set[Set[String]]()

  var processedConceptsIntent = Set[Set[String]]()
  var parent_index = 0
  var parent_positive_index = 1
  var parent_negative_index = 1
  def main(args: Array[String]): Unit = {

    println("hello world 3way data reader")

    val inputFile = args(0)
    implicit val ec = ExecutionContext.global
    //val inputFile = "/Users/raghavendrakumar/Downloads/FCA/final_tableau.csv"

    /*   val sparkContext = SparkContext.getOrCreate(new SparkConf().setAppName("cg1").setMaster("local[*]").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  .set("spark.kryo.registrationRequired", "true")
   .set("spark.kryoserializer.buffer.mb","8")
  .registerKryoClasses(
        Array(
          classOf[Concept],
          classOf[Array[Concept]],
          classOf[scala.collection.mutable.WrappedArray.ofRef[_]],
          classOf[scala.collection.immutable.Map[_, _]],

          classOf[scala.collection.immutable.Set[_]],
          // Class.forName("scala.collection.immutable.Set")
          Class.forName("scala.collection.immutable.MapLike$ImmutableDefaultKeySet"),
          Class.forName("scala.collection.immutable.HashMap$HashMap1"),
          Class.forName("scala.collection.immutable.HashMap$EmptyHashMap$"),
          Class.forName("org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage"),
          classOf[Array[Array[Byte]]],
            classOf[Array[Object]],
          classOf[Array[org.apache.spark.sql.Row]],
          classOf[org.apache.spark.sql.catalyst.expressions.GenericRow],
          classOf[org.apache.spark.sql.catalyst.InternalRow],
          classOf[Array[org.apache.spark.sql.catalyst.InternalRow]],
            Class.forName("scala.collection.immutable.Set$EmptySet$"),
          classOf[org.apache.spark.sql.catalyst.expressions.UnsafeRow])))

    val sparkContext1 = SparkContext.getOrCreate(new SparkConf().setAppName("cg2").set("spark.executor.memory", "140g").setMaster("local[*]")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrationRequired", "true")
         .set("spark.kryoserializer.buffer.mb","8")
      .registerKryoClasses(
        Array(
          classOf[Concept],
          classOf[Array[Concept]],
          classOf[scala.collection.mutable.WrappedArray.ofRef[_]],
          classOf[scala.collection.immutable.Map[_, _]],
          classOf[scala.collection.immutable.Set[_]],
          Class.forName("scala.collection.immutable.Set$EmptySet$"),
          Class.forName("scala.collection.immutable.MapLike$ImmutableDefaultKeySet"),
          Class.forName("scala.collection.immutable.HashMap$HashMap1"),
           Class.forName("scala.collection.immutable.HashMap$EmptyHashMap$"),
           Class.forName("org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage"),
          classOf[Array[Array[Byte]]],
          classOf[Array[org.apache.spark.sql.Row]],
           classOf[Array[Object]],
          classOf[org.apache.spark.sql.catalyst.expressions.GenericRow],
          classOf[org.apache.spark.sql.catalyst.InternalRow],
          classOf[Array[org.apache.spark.sql.catalyst.InternalRow]],
          classOf[org.apache.spark.sql.catalyst.expressions.UnsafeRow])))*/

    val sparkContext = SparkContext.getOrCreate(new SparkConf().setAppName("cg1").setMaster("local[*]"))

    val sparkContext1 = SparkContext.getOrCreate(new SparkConf().setAppName("cg2").setMaster("local[*]"))

    val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)
    val df = sqlContext.read
      .option("delimiter", args(1))
      .option("header", "true")
      .csv(inputFile).cache().coalesce(10000)

    val attributesinContext = sparkContext.parallelize(Seq(df.columns.filter(_ != "_c0")))
    val allAttributes = attributesinContext.reduce { (a, b) => a.union(b) }.toSet

    //conceptGeneration(sparkContext, sqlContext, df, allAttributes, "1", "pc/pc_1")
    //conceptGeneration(sparkContext1, sqlContext, df, allAttributes, "0", "nc/pc_2")

    val positiveContext = getContext(df, "1")
    //  df.show()
    val positiveContextAsMap = positiveContext.collect().toMap
    val positiveContextKeySet = positiveContextAsMap.keySet

    val positiveContextTranspose = getContextTranspose(sparkContext, sqlContext, df, "1")

    val positiveContextTransposeAsMap = positiveContextTranspose.collect().toMap

    val negativeContext = getContext(df, "0")
    val negativeContextAsMap = negativeContext.collect().toMap
    val negativeContextKeySet = negativeContextAsMap.keySet

    val negativeContextTranspose = getContextTranspose(sparkContext1, sqlContext, df, "0")
    val negativeContextTransposeAsMap = negativeContextTranspose.collect().toMap

    new Thread {
      override def run {
        conceptGeneration(sparkContext, positiveContext, positiveContextAsMap, positiveContextTransposeAsMap, allAttributes,
          true, positiveContextKeySet, args(2))
        val positiveConcepts = sparkContext.textFile(args(2) + "*/part-[0-9]*").
          distinct().map { f =>
            val data = f.replaceAll("Concept" + "\\(", "").replaceAll("\\," + "true" + "\\)", "").replaceAll("\\)\\,", "\\)\\#")
             .split("\\#")
           (data(0).replaceAll("Set" + "\\(", "").replaceAll("\\)", "").split(",").mkString ,
            data(1).replaceAll("Set" + "\\(", "").replaceAll("\\)", "").split(",").toSet, data(2).replaceAll("[a-z]*,","").replace(")", ""))
        }.repartition(1).cache
        positiveConcepts.saveAsTextFile(args(6))
        getAEConcepts(positiveConcepts, negativeContextTransposeAsMap, "1", args(5) + "1")
       getOEConcepts(positiveConcepts, negativeContextAsMap, "1", args(4) + "1")
      }
    }.start()

    new Thread {
      override def run {
        conceptGeneration(sparkContext1, negativeContext, negativeContextAsMap, negativeContextTransposeAsMap, allAttributes, false, negativeContextKeySet, args(3));

        val negativeConcepts = sparkContext1.textFile(args(3) + "*/part-[0-9]*").
          distinct().map { f =>
            val data = f.replaceAll("Concept" + "\\(", "").replaceAll("\\," + "true" + "\\)", "").replaceAll("\\)\\,", "\\)\\#")
        
             .split("\\#")
           (data(0).replaceAll("Set" + "\\(", "").replaceAll("\\)", "").split(",").mkString ,
            data(1).replaceAll("Set" + "\\(", "").replaceAll("\\)", "").split(",").toSet, data(2).replaceAll("[a-z]*,","").replace(")", ""))
             
          }.repartition(1).cache()
          negativeConcepts.saveAsTextFile(args(7))
         getOEConcepts(negativeConcepts, positiveContextAsMap, "0", args(4) + "2")
        getAEConcepts(negativeConcepts, positiveContextTransposeAsMap, "0", args(5) + "2")
      }

    }.start()

    // sparkContext.parallelize(Seq(processedConceptsIntent)).saveAsTextFile(" Concepts Processed ")

    //println("Negative concepts size" + negativeConcepts.count())

    println("total concepts" + parent_index)

    // Await.result(f,Duration.Inf)
    // Await.result(f1,Duration.Inf)

    // println("Positive concepts size" + positiveConcepts.count())

  }

  def conceptGeneration(sparkContext: SparkContext, context: RDD[(String, Set[String])], contextAsMap: Map[String, Set[String]],
                        contextTransposeAsMap: Map[String, Set[String]], allAttributes: Set[String], isGeneratedConceptPositive: Boolean, keySet: Set[String], opfile: String) {

    val leastFormalConcept = getLeastFormalContext(contextTransposeAsMap, allAttributes, true, 1)
    val minSet = getMinSet(context, leastFormalConcept)
    getFormalConceptforEachObject(sparkContext, leastFormalConcept, contextTransposeAsMap, contextAsMap, minSet, Set(leastFormalConcept), true,
      isGeneratedConceptPositive, keySet, opfile)
    println("concept generation job done")

  }

  def getContext(df: DataFrame, attributeValue: String): RDD[(String, Set[String])] = {
    (df.rdd.map { r =>
      (
        r.getString(0),
        r.getValuesMap[String](r.schema.fieldNames.filter(_ != "_c0")).filter(p => p._2 == attributeValue).keySet)
    })

  }

  def getContextTranspose(sc: SparkContext, sqlContext: SQLContext, df: DataFrame, attributeValue: String): RDD[(String, Set[String])] = {

    val (header, data) = df.collect.map(_.toSeq.toArray).transpose match {
      case Array(h, t @ _*) => {
        (h.map(_.toString), t.map(_.collect { case x: String => x }))
      }
    }
    val rows = sc.parallelize((df.columns.tail.zip(data).map { case (x, ys) => Row.fromSeq(x +: ys) }))
    val schema = StructType(
      StructField("vals", StringType) +: header.map(StructField(_, StringType)))
    sqlContext.createDataFrame(rows, schema)
    sqlContext.createDataFrame(rows, schema).rdd.map { r =>
      (r.getString(0),
        r.getValuesMap[String](r.schema.fieldNames.filter(_ != "vals")).filter(p => p._2 == attributeValue).keySet)
    }

  }

  def getLeastFormalContext(complimentContextAsMap: Map[String, Set[String]], allAttributes: Set[String], isValidNeighbor: Boolean, parent_index: Integer): Concept = {

    (Concept(allAttributes.map(f => complimentContextAsMap.get(f)).flatten.reduceOption { (a, b) => a.intersect(b) }.toSet.flatten, allAttributes, isValidNeighbor, 1))
    // (Concept(allAttributes.map(f => complimentContextAsMap.get(f)).flatten.foldLeft(Set[String]())(_ intersect _), allAttributes, isValidNeighbor))

  }

  def getMinSet(context: RDD[(String, Set[String])], leastFormalConcept: Concept): Set[String] = {
    val minSet = context.map { f =>
      val objectsInContext = f._1
      (objectsInContext)
    }.collect().toSet.diff(leastFormalConcept.extent)
    (minSet)
  }

  def getFormalConceptforEachObject(sc: SparkContext, concept: Concept, contextInverseAsMap: Map[String, Set[String]], contextAsMap: Map[String, Set[String]],
                                    minSet: Set[String], validUpperNeighborConcepts: Set[Concept], isLeastConcept: Boolean, isGeneratedConceptPositive: Boolean, keySet: Set[String], opfile: String): Any = {

    val format = new SimpleDateFormat("ddHHmmssSSS")

    val concepts = if (isGeneratedConceptPositive) generatedPositiveconceptIntent else generatedNegativeconceptIntent

    val isIntentAlreadyPresent = concepts.apply(concept.intent)
    val validConcepts = if (concept.isValidNeighbor && !(isIntentAlreadyPresent)) {

      // println("entered with concept details" + concept.extent + concept.intent + concept.isValidNeighbor)

      getPositiveOrNegativeConcept(isGeneratedConceptPositive, concept.intent)

      val differenceSet = sc.parallelize(keySet.diff(concept.extent).toSeq)

      val validConcept = differenceSet.map { currentObject =>

        processedConceptsIntent = processedConceptsIntent + concept.intent

        val commonObjectAsSet = Set(currentObject)
        val objectsRequiredforB1 = concept.extent.union(commonObjectAsSet)

        val forEachObjectInB1 = objectsRequiredforB1.map {
          p =>
            val attributesForEachObjectAsString = contextAsMap.apply(p)

            (attributesForEachObjectAsString)
        }

        val B1 = forEachObjectInB1.reduceOption { (a, b) => (a.intersect(b)) }.toSet.flatten
        val listOfIntents = validUpperNeighborConcepts.map { concept =>
          val intentOfConcept = concept.intent
          (intentOfConcept)
        }.flatten

        val validNeighbors = if (!(B1.isEmpty) || isLeastConcept) {

          val B1asRDD = B1.map {
            f =>
              val rddElementasSet = Set(f)
              (rddElementasSet)
          }

          //if lenght is one consider the value from contextInverseAsMap
          val conceptsFound = if (B1.size == 1) {
            val a = B1asRDD.map { f =>
              val singleRDD = f.map { g =>
                val data = g
                (data)
              }.mkString
              (singleRDD)
            }

            if (isGeneratedConceptPositive)
              (Concept(contextInverseAsMap.get(a.take(1).mkString).toSet.flatten, B1, true, parent_positive_index))
            else
           
            (Concept(contextInverseAsMap.get(a.take(1).mkString).toSet.flatten, B1, true, parent_negative_index))
          } else {

            val conceptExtentEach = B1asRDD.map { f =>

              val extentData = contextInverseAsMap.apply(f.mkString)
              // println(extentData)
              (extentData)
            }.reduceOption { (a, b) => a.intersect(b) }.toSet.flatten

            //reduce { (a, b) => a.intersect(b) } 
            if (isGeneratedConceptPositive)
            {
            
            (Concept(conceptExtentEach, B1, true, parent_positive_index))
            }
            else{
          
            (Concept(conceptExtentEach, B1, true, parent_negative_index))
            }
          }

          val differenceinComparisonSet = (conceptsFound.extent.diff(concept.extent)).diff(commonObjectAsSet)
          val validNeighborConcept = if (minSet.intersect(differenceinComparisonSet).size == 0) {
            if (isGeneratedConceptPositive){
               
             (Concept(conceptsFound.extent, B1, true, parent_positive_index))
            }
            else{
             
            (Concept(conceptsFound.extent, B1, true, parent_negative_index))
            }
          } else {
            minSet.-(currentObject)

            if (isGeneratedConceptPositive){
             Concept(conceptsFound.extent, B1, true, parent_positive_index)
            }
            else{
         
            Concept(conceptsFound.extent, B1, true, parent_negative_index)
            }

          }
          
      

          (validNeighborConcept)
        } else {
           if (isGeneratedConceptPositive){
               (Concept(Set(), Set(), false,0))
             
           }
          //parent_index =parent_index -1
           else{
          (Concept(Set(), Set(), false,0))
           }

        }
        
       
        (validNeighbors)
        
        
         
        
      }
      val validConcpetsForFile = validConcept.filter(f => f.isValidNeighbor == true)

      //&& !(concepts.apply(f.intent))
      // if (validConcpetsForFile.count() != 0) {

      // if (!(validConcpetsForFile.isEmpty())) {
        if (isGeneratedConceptPositive){
             parent_positive_index = parent_positive_index +1
          
        
               validConcpetsForFile.saveAsTextFile(opfile + "_" + System.currentTimeMillis())

        }
            else
            {
           //   parent_negative_index = parent_negative_index +1
              
               validConcpetsForFile.saveAsTextFile(opfile + "_" + System.currentTimeMillis())

            }
     
      //validConcept.foreach(println)
      // validConcpetsForFile.cache()
     
      //   print("data")

      // }

      
      
      (validConcept)
    } else {
      return

    }

    val validConceptsInSet = validConcepts.toLocalIterator
    var canRun = true

    while (validConceptsInSet.hasNext && canRun && concept.isValidNeighbor && !(isIntentAlreadyPresent)) {
      val partitionNumber = validConceptsInSet.next()

      if (!(concepts.apply(partitionNumber.intent))) {
        
     
        
      
       

        getFormalConceptforEachObject(sc, partitionNumber, contextInverseAsMap, contextAsMap, minSet, validUpperNeighborConcepts, false, isGeneratedConceptPositive, keySet, opfile)

      }
    }

  }

  def getPositiveOrNegativeConcept(isPositiveConceptGenerated: Boolean, conceptIntent: Set[String]): Set[Set[String]] = {

    if (isPositiveConceptGenerated) {

      generatedPositiveconceptIntent = generatedPositiveconceptIntent + conceptIntent
      (generatedPositiveconceptIntent)
    } else {

      generatedNegativeconceptIntent = generatedNegativeconceptIntent + conceptIntent

      (generatedNegativeconceptIntent)
    }

  }

  def getOEConcepts(concepts: RDD[(String, Set[String], String)], contextMap: Map[String, Set[String]], flag: String, opfile: String): Unit = {

    concepts.map { f =>
      val op = f._1.split(" ").flatMap { f =>

        (contextMap.get(f))
        // }.reduce { (a, b) => a.intersect(b) }

      }.reduceOption { (a, b) => a.intersect(b) }.toSet.flatten
      if (flag == "1")
        (Set(f._1)+"#", f._2+"#", op.toSet.diff(f._2)+"#",f._3)
      else
        (Set(f._1)+"#", op.toSet+"#", f._2.diff(op.toSet)+"#",f._3)

    }.saveAsTextFile(opfile)
  }

  def getAEConcepts(concepts: RDD[(String, Set[String],String)], contextMap: Map[String, Set[String]], flag: String, opfile: String): Unit = {
    concepts.map { f =>

      val data = f._2.mkString.split(" ").map {

        f =>
          val data = contextMap.get(f).mkString.
            replaceAll("Set" + "\\(", "").replaceAll("\\)", "").replaceAll(" ", "").
            split(",").toSet

          (data)
      }.toSet.reduceOption { (a, b) => a.intersect(b) }.toSet.flatten

      //reduce { (a, b) =>

      //a.intersect(b)
      //}

      if (flag == "1")
        (f._1+"#", Set(data)+"#", f._2+"#",f._3)
      else
        (Set(data)+"#", f._1+"#", f._2+"#",f._3)

    }.saveAsTextFile(opfile)

  }
}
