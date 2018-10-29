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

Projection实现思路：</br>
只需要在Map阶段处理读入的每一行Text，将其转化为字符串</br>
然后将字符串分割，把得到的字符串数组的第二个元素（即name）提取出来</br>
然后将其转为Text</br>
最后在Map阶段输出(name, NullWritable.get())

Union实现思路：</br>
在Map阶段读入两个关系文本，并以(record, IntWritable)的形式传给Reduce阶段</br>
在Reduce阶段输出(record, NullWritable.get())即可

Intersection实现思路：</br>
参考Union，在Reduce阶段，只输出值>1的键即可

Difference实现思路：</br>
计算setA-setB，对于A、B中的每一条记录r，在Map阶段分别输出键值对(r, A)和(r, B)</br>
在Reduce阶段检查一条记录r的所有对应值列表，如果只有A而没有B，则将该记录输出

NatureJoin实现思路：</br>
在Map阶段读取两个关系文本，以(id, (other_attributes, 1))和(id, (other_attributes, 2))输出</br>
然后在Reduce阶段将id相同的键值对中的值连接起来再输出即可
