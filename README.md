# FBDP_Homework_4
Created by YHR Oct. 29th 2018. All rights reserved

/source_code/WordCount.java以及/source_code/MatrixMultiply.java是由书本提供的源代码修改得来的；
关系代数部分代码全由本人独立编写

Slection实现思路是：
在Map阶段，判断每一行输入，若符合条件（age >= 18）则将满足得记录输出即可
输出得键值对为（record， NullWritable.get())，无需Reduce阶段，其中record为Text类型

Projection实现思路：
