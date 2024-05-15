package java_jdbc;

import java.sql.*;

public class java_jdbc_insert {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        //todo  1.注册驱动,这个为静态方法调用类名.
       Class.forName("com.mysql.cj.jdbc.Driver");
        //todo  2. 获取连接【connection建立连接】
        Connection connection = DriverManager.getConnection("jdbc:mysql://master:3306/?serverTimezone=UTC&characterEncoding=utf8&useUnicode = true", "root", "123456");
        //todo  3. 创建发送sql语句对象【statement 创建发送sql语句的statement】
        String sql = "insert into  atguigu.t_user(account, password, nickname) values (?, ?, ?)";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);

        //todo  4. 发送sql语句，并获取返回结果【statement发送sql语句到数据库 并且取得返回结构】
        //TODO 在批量插入的时候需要大量的时间，可以优化批量插入
        for ( int i =10; i <10000 ;i++) {
            preparedStatement.setObject(1, "dddqq" +i);
            preparedStatement.setObject(2, "123456");
            preparedStatement.setObject(3, "经理");
            //TODO 将语句的values增加，不执行插入在增加结束后面执行
            preparedStatement.addBatch();
        }
        //TODO 执行bath的任务就行批量加载数据
        preparedStatement.executeBatch();

        System.out.println("--------------------------------");
    }
}
