# FBDP_Homework_4
Created by YHR Oct. 29th 2018. All rights reserved

/source_code/WordCount.java以及/source_code/MatrixMultiply.java
均由书本提供的源代码修改得来的；

关系代数部分代码全由本人独立编写

Slection实现思路是：</br>
在Map阶段，判断每一行输入，若符合条件（age >= 18）则将满足得记录输出即可</br>
输出得键值对为（record， NullWritable.get())，无需Reduce阶段，其中record为Text类型</br>
执行命令为：</br>
./bin/hadoop jar Selection.jar Selection input output 2 18</br>
其中2为关系文本的第三列（即年龄），18为年龄限制条件</br>
具体判断逻辑请参见源代码RelationA类中的isCondition方法</br>

Projection实现思路：
