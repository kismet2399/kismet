package com.badou

import org.apache.spark.sql.DataFrame

object work {
  def Feat(priors:DataFrame, orders: DataFrame):DataFrame={
    /**
      *
      */
    val prodCnt = priors.groupBy("product_id").count()

  }
}
