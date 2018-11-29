package com.badou.study

class ModelOrderingTwo extends Ordering[Model] {
  override def compare(x: Model, y: Model): Int = {
    if (x.name == y.name) {
      x.age - y.age
    } else if (x.name > y.name) {
      1
    } else {
      -1
    }
  }
}
