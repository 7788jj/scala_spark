package sparkday03
/*
聚合函数最重要的就是中间临时变量在实际情况当中来决定,只要有聚合函数接必须有中间变来；量；

 */
object sparkRDD_reduce_fold {
  def main(args: Array[String]): Unit = {
    val ints: Seq[Int] = Seq(1, 2, 3, 4, 5)

    //TODO reduce函数的使用（tmp表示临时变量， iter表示集合的每一个元素）
     val int = ints.reduce((tmp , iter) =>
      (tmp + iter)
    )
    println(int)

    //TODO dold折叠函数（）（）需要传递两个参数，第一个表示临时变量的初始值（后面两个表示临时变量， ite表示集合的每一个元素）
    val i: Int = ints.fold(0)((tmp, iter) => (tmp + iter))
    println(i)


  }

}
