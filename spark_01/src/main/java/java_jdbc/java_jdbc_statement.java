package java_jdbc;

import com.mysql.cj.jdbc.Driver;

import java.sql.*;

/*
java的jdbc固定链接步骤
TODO 1. 注册驱动【依赖的jar包 进行安装】
TODO 2. 获取连接【connection建立连接】
TODO 3. 创建发送sql语句对象【statement 创建发送sql语句的statement】
TODO 4. 发送sql语句，并获取返回结果【statement发送sql语句到数据库 并且取得返回结构】
TODO 5. 结果集解析【将result结果解析出来】
TODO 6. 资源关闭【释放resultset、statement、connection】
 */
public class java_jdbc_statement {
    public static void main(String[] args) throws SQLException {
        //todo  1.注册驱动,这个为静态方法调用类名.
        DriverManager.deregisterDriver(new Driver());

        //todo 2.创建链接
        Connection connection = DriverManager.getConnection("jdbc:mysql://master:3306/atguigu", "root", "123456");

        //todo 3.创建statement对象
        Statement statement = connection.createStatement();

        //todo 4.发送语句
        String sql = "select id, account, password, nickname from t_user";
        //这个会返回一个结果集
        ResultSet resultSet = statement.executeQuery(sql);

        //todo 5.解析结果集
        while (resultSet.next()){
            int id = resultSet.getInt("id");
            String account = resultSet.getString("account");
            String password = resultSet.getString("password");
            String nickname= resultSet.getString("nickname");
            System.out.println(id+ "--"+account +"--"+password +"--"+nickname);
//            1--root--123456--经理
//            2--admin--666666--管理员


        }

        //6.TODO 关闭资源由内而外关闭
        resultSet.close();
        statement.close();
        connection.close();




    }
}
