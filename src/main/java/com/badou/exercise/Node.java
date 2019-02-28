package com.badou.exercise;

import lombok.Getter;
import lombok.Setter;

/**
 * @description:
 * @author: zuo.zhuan
 * @create: 2019-02-27 10:08
 */
@Getter
@Setter
public class Node {
    private int data;// 数据域
    private Node Next;// 指针域

    public Node(int data) {
        this.data = data;
    }
}