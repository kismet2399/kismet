package com.badou.study

class Fraction[T: Ordering](val a: T, val b: T) {
  def small(implicit order: Ordering[T]) = {
    if (order.compare(a, b) < 0) println(a.toString) else println(b.toString)
  }
}
