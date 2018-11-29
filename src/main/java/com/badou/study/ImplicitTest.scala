package com.badou.study

object ImplicitTest {

  implicit def intToString(i:Int)=i.toString

  def func(msg:String) = println(msg)

  def +(msg:String) = println(msg)

}
