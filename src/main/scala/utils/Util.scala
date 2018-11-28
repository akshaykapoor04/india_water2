package utils

import org.apache.spark.sql.functions.udf

object Util {
  val convertToLowerCase = udf((state: String) => {
    if(state!=null&&state!="null"&&state!="")
      {
        state.toLowerCase()
      }
    else
      {
        "null"
      }
  })
  val removeCodes = udf((district: String) => {
    if(district!=null&&district!="null"&&district!=""&&district.contains("("))
    {
      district.split("\\(")(0)
    }
    else
    {
      district
    }
  })
  val getYear = udf((date: String) => {
    if(date!=null&&date!="null"&&date!=""&&date.contains("/"))
    {
      val dateArr=date.split("/")
      dateArr(dateArr.length-1)
    }
    else
    {
      date
    }
  })
  val changeSpecial = udf((data: String) => {
    if(data!=null&&data!="null"&&data!=""&&data.contains("&"))
    {
      data.replaceAll("&","and")
    }
    else
    {
      data
    }
  })
  val changeDistrictCode = udf((data: Integer) => {
    if(data==null||data==0)
    {
      val default:Integer=(-999)
      println(default)
      default
    }
    else
    {
      data
    }
  })

}
