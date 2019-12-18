const mongoose = require('mongoose');

async function query() {
  await mongoose.connect(
    'mongodb://keanuo:19980411@localhost:27017/talent?authSource=talent',
    {
      useNewUrlParser: true,
      useUnifiedTopology: true
    }
  );

  let studentSchema = new mongoose.Schema({
    考生号: String,
    考生姓名: String,
    身份证号: String,
    性别代码: String,
    出生日期: String,
    毕业类别代码: String,
    户籍地代码: String,
    录取院校代码: String,
    录取院校名称: String,
    录取专业名称: String,
    录取层次代码: String,
    录取标准代码: String,
    录取批次代码: String,
    录取日期: String
  });

  let studentModel = mongoose.model(
    '关联-高考库报名录取信息表',
    studentSchema,
    '关联-高考库报名录取信息表'
  );

  let registerSchema = mongoose.Schema({
    考生号: String,
    考生姓名: String,
    报名所持有效证件号码: String,
    性别代码: String,
    出生日期: String,
    毕业类别代码: String,
    户籍地代码: String
  });

  let registerForm = mongoose.model(
    'register_forms',
    registerSchema,
    '新高考库_考生报名信息表'
  );

  let enrollSchema = mongoose.Schema({
    考生号: String,
    考生姓名: String,
    录取院校代码: String,
    录取院校名称: String,
    录取专业名称: String,
    录取层次代码: String,
    录取标准代码: String,
    录取批次代码: String,
    录取日期: String
  });

  let enrollForm = mongoose.model(
    'enroll_forms',
    enrollSchema,
    '新高考库_考生录取情况表'
  );

  let count = 1;
  const cursor = enrollForm.find({}).cursor();
  let doc;
  while ((doc = await cursor.next())) {
    const form = await registerForm.findOne({ 考生号: doc.考生号 });
    if (form) {
      const student = {
        考生号: form.考生号,
        考生姓名: form.考生姓名,
        身份证号: form.报名所持有效证件号码,
        性别代码: form.性别代码,
        出生日期: form.出生日期,
        毕业类别代码: form.毕业类别代码,
        户籍地代码: form.户籍地代码,
        录取院校代码: doc.录取院校代码,
        录取院校名称: doc.录取院校名称,
        录取专业名称: doc.录取专业名称,
        录取层次代码: doc.录取层次代码,
        录取标准代码: doc.录取标准代码,
        录取批次代码: doc.录取批次代码,
        录取日期: doc.录取日期
      };
      const stu = new studentModel(student);
      await stu.save();
    } else continue;
    console.log(`${count++}`);
  }

  mongoose.disconnect();
}

query();
