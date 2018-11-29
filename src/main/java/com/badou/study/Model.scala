package com.badou.study

class Model(val name:String, val age:Int) {
  println(s"构造对象 {$name,$age}")
}

object Model{
  implicit def mo = new ModelOrdering
  val f = new Fraction(new Model("Shelly",28),new Model("Alice",35))
  f.small
}

