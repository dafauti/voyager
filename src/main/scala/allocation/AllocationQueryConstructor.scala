package com.gmail.bigdata.dpp.allocation

import java.util.StringJoiner

import com.gmail.bigdata.dpp.driver.AllocationJobDriver.{ AllocationSteps}



/**
 * dynamically creates allocation and dropout queries based on input parameters
 *
 */
class AllocationQueryConstructor {

  def getAllocationQuery (masterTable: String,allocationTable: String,allocateBy: String,groupby_literal: String,col_name: String) : String = {

    var join_condition_1=""
    var join_condition_2=""

    var  joincondition =groupby_literal.split(",")
    for( arg <- joincondition ){
      if (join_condition_1.length>0 && join_condition_2.length>0) {
        join_condition_1 = join_condition_1 + " AND a." + arg + "=b." + arg
        join_condition_2 = join_condition_2 + " AND a." + arg + "=c." + arg
      }
      else {
        join_condition_1="a."+arg+"=b."+arg
        join_condition_2="a."+arg+"=c."+arg
      }
    }


    s"""  SELECT  a.*,
      COALESCE((CAST($allocateBy AS DECIMAL(38,16))*COALESCE(CAST(allocation_value AS DECIMAL(38,16)), CAST(0 AS DECIMAL(38,16))))/group_sum , CAST(0 AS DECIMAL(38,16))) AS $col_name
      FROM $masterTable a
      LEFT JOIN
      aggregated_view b
      ON $join_condition_1
      LEFT JOIN $allocationTable c
      ON $join_condition_2"""


  }

  def getL1AllocationQuery(masterTable: String,configDataList:List[AllocationSteps]): String = {

    val sb1 = new StringJoiner(",\n")
    val sb2 = new StringBuilder
    for(conf<-configDataList){
      sb1.add("COALESCE((a."+conf.allocate_by+"/SUM(a."+conf.allocate_by+") OVER (PARTITION BY "+getQueryHelper(conf.l1_group_by,conf.allocation_file).partitionByQuery+"))*"+conf.allocation_file.split('.')(1)+".allocation_value,0) AS "+conf.column_name)
      sb2.append("LEFT JOIN "+conf.allocation_file+" ON "+getQueryHelper(conf.l1_group_by,conf.allocation_file).joinCondition+"\n")
    }
    val finalQuery=new StringBuilder
    finalQuery.append("SELECT a.*,\n"+sb1.toString()+"\nFROM "+masterTable+" a \n").append(sb2.toString())


    finalQuery.toString()

  }

  def getL2AllocationQuery(masterTable: String,configDataList:Array[AllocationSteps]): String = {

    val sb1 = new StringJoiner(",\n")
    val sb2 = new StringBuilder
    for(conf<-configDataList){
      sb1.add("COALESCE((a."+conf.allocate_by+"/SUM(a."+conf.allocate_by+") OVER (PARTITION BY "+getQueryHelper(conf.l2_group_by.get,conf.allocation_file).partitionByQuery+"))*"+conf.allocation_file+".allocation_value,0) AS "+conf.column_name+"_l2")
      //sb1.add("COALESCE((a."+conf.allocate_by+"/SUM(a."+conf.allocate_by+") OVER (PARTITION BY "+getQueryHelper(conf.l2_group_by.get,conf.allocation_file).partitionByQuery+"))*"+conf.allocation_file.split('.')(1)+".allocation_value,0) AS "+conf.column_name+"_l2")
      sb2.append("LEFT JOIN "+conf.allocation_file+" ON "+getQueryHelper(conf.l2_group_by.get,conf.allocation_file).joinCondition+"\n")
    }
    val finalQuery=new StringBuilder
    finalQuery.append("SELECT a.*,\n"+sb1.toString()+"\nFROM "+masterTable+" a \n").append(sb2.toString())


    finalQuery.toString()

  }


  def getQueryHelper(groupby_literal: String,allocation_file: String): QueryHelper ={
    var  joinconditions =groupby_literal.split(",")
    var join_condition=""
    var partitionByQuery=""
    for( arg <- joinconditions ){
      if (join_condition.length>0) {
        join_condition = join_condition + " AND a." + arg + "="+allocation_file+"."+ arg
        //join_condition = join_condition + " AND a." + arg + "="+allocation_file.split('.')(1)+"."+ arg
        partitionByQuery=partitionByQuery+",a."+arg
      }
      else {
        join_condition="a."+arg+"="+allocation_file+"."+arg
        //join_condition="a."+arg+"="+allocation_file.split('.')(1)+"."+arg
        partitionByQuery="a."+arg
      }
    }
    QueryHelper(join_condition,partitionByQuery)

  }
  case class QueryHelper(joinCondition: String, partitionByQuery: String)

  def getAllocationQuery_withbroadcast (masterTable: String,allocationTable: String,allocateBy: String,groupby_literal: String,col_name: String) : String = {

    var join_condition_1=""
    var join_condition_2=""

    var  joincondition =groupby_literal.split(",")
    for( arg <- joincondition ){
      if (join_condition_1.length>0 && join_condition_2.length>0) {
        join_condition_1 = join_condition_1 + " AND a." + arg + "=b." + arg
        join_condition_2 = join_condition_2 + " AND a." + arg + "=c." + arg
      }
      else {
        join_condition_1="a."+arg+"=b."+arg
        join_condition_2="a."+arg+"=c."+arg
      }
    }

    s"""  SELECT /*+ Broadcast(b,c) */ a.*,
          COALESCE((CAST($allocateBy AS DECIMAL(38,16))*COALESCE(CAST(allocation_value AS decimal(38,16)), CAST(0 AS DECIMAL(38,16))))/group_sum , CAST(0 AS DECIMAL(38,16))) AS $col_name
          FROM $masterTable a
          LEFT JOIN
          aggregated_view b
          ON $join_condition_1
          LEFT JOIN $allocationTable c
          ON $join_condition_2"""

  }



  def getAggAllocationQuery (masterTable: String,allocationTable: String,allocateBy: String,groupby_literal: String,col_name: String) : String = {


    s"""
      SELECT $groupby_literal,SUM(COALESCE(CAST($allocateBy AS DECIMAL(38,16)), CAST(0 AS DECIMAL(38,16)))) AS group_sum FROM $masterTable GROUP BY $groupby_literal
    """
  }


  def getDropOutQuery (masterTable: String,allocationTable: String,allocateBy: String,join_literal: String,select_literal: String,yearRange:String) : String = {

    var join_condition_1=""
    var select_string=""

    var  joincondition =join_literal.split(",")
    for( arg <- joincondition ){
      if (join_condition_1.length>0) {
        join_condition_1 = join_condition_1 + " AND a." + arg + "=b." + arg
      }
      else {
        join_condition_1="a."+arg+"=b."+arg
      }
    }

    var  selectCondition =select_literal.split(",")
    for( arg <- selectCondition ){
      if (select_string.length>0) {
        select_string= select_string+",a."+arg
      }
      else {
        select_string= "a."+arg
      }
    }


    s"""SELECT  $select_string,SUM(COALESCE(CAST(a.allocation_value AS DECIMAL(38,16)), CAST(0 AS DECIMAL(38,16)))) AS allocation_value FROM (SELECT * FROM $allocationTable WHERE fiscal_year IN $yearRange) a
      LEFT JOIN
      $masterTable b ON $join_condition_1
    WHERE b.fiscal_month IS NULL AND a.allocation_value IS NOT NULL
    GROUP BY $select_string"""
  }

  def getDropOutQuery_withbroadcast (masterTable: String,allocationTable: String,allocateBy: String,join_literal: String,select_literal: String) : String = {

    var join_condition_1=""
    var select_string=""

    var  joincondition =join_literal.split(",")
    for( arg <- joincondition ){
      if (join_condition_1.length>0) {
        join_condition_1 = join_condition_1 + " AND a." + arg + "=b." + arg
      }
      else {
        join_condition_1="a."+arg+"=b."+arg
      }
    }

    var  selectCondition =select_literal.split(",")
    for( arg <- selectCondition ){
      if (select_string.length>0) {
        select_string= select_string+",a."+arg
      }
      else {
        select_string= "a."+arg
      }
    }

    s"""SELECT /*+ Broadcast(a) */ $select_string,SUM(COALESCE(CAST(a.allocation_value AS DECIMAL(38,16)), CAST(0 AS DECIMAL(38,16)))) AS allocation_value FROM $allocationTable a
          LEFT JOIN
          $masterTable b ON $join_condition_1
        WHERE b.fiscal_month IS NULL AND a.allocation_value IS NOT NULL
        GROUP BY $select_string"""

  }
}
