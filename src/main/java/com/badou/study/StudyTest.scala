package com.badou.study

import scala.math._
import com.badou.study.ImplicitTest._

object StudyTest {
  def main(args: Array[String]) {
    println(forSqrt(sqrt, 0.25))
    func(111);
  }

  def forSqrt(f: (Double) => Double, params: Double) = {
    10 + f(params)
  }

  def addBy(factor:Int) = {
    (x:Double)=> factor + x
  }
}
