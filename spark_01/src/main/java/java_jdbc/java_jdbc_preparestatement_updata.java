package java_jdbc;

import java.sql.*;

/*
TODO jdbc更新与插入数据
 */
public class java_jdbc_preparestatement_updata {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        //1.利用反射创建jdbc驱动
        //TODO 在这里面的Driver类会加载静态代码块创建驱动类，注意这个为Driver类的全路径名称
        Class.forName("com.mysql.cj.jdbc.Driver");

        //2.获取链接
        Connection connection = DriverManager.getConnection("jdbc:mysql://master:3306/?serverTimezone=UTC&characterEncoding=utf8&useUnicode = true", "root", "123456");

        String sql = "insert into  atguigu.t_user(account, password, nickname) values (?, ?, ?)";
        //3.创建statement对象
        PreparedStatement preparedStatement = connection.prepareStatement(sql);

        //4.给占位符赋值，发送sql语句
        preparedStatement.setObject(1,"atguigu");
        preparedStatement.setObject(2,"000000");
        preparedStatement.setObject(3,"部员");
        int i = preparedStatement.executeUpdate(sql);
        if(i > 0){
            System.out.println("更新成功");
        }else {
            System.out.println("更新失败");
        }

        //5.关闭资源
        preparedStatement.close();
        connection.close();
    }

}
