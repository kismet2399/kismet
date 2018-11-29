package com.badou.study

class IntWritable(_value: Int) {
  class IntWritable(_value:Int){
    def value = _value
    def +(that:IntWritable): IntWritable ={
      new IntWritable(that.value + value)
    }
  }
  implicit  def intToWritable(int:Int)= new IntWritable(int)
}
