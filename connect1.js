const mongoose = require('mongoose');

async function query() {
  await mongoose.connect(
    'mongodb://keanuo:19980411@localhost:27017/talent?authSource=talent',
    {
      useNewUrlParser: true,
      useUnifiedTopology: true
    }
  );

  let ssSchema = mongoose.Schema({
    个人编号: String,
    证书情况: Array,
    转移历史: Array,
    单位编号: String,
    姓名: String,
    性别: String,
    公民身份号码: String,
    文化程度: String,
    进入年份: String,
    退出年份: String,
    转出地域: String,
    证书等级: String,
    '国家职业资格等级（工人技术等级）]': String,
    专业技术职务: String,
    '行政职务(级别)': String,
    社保所在地区: String
  });

  let ssForm = mongoose.model(
    'new1-社保库个人信息表',
    ssSchema,
    'new1-社保库个人信息表'
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

  let studentForm = mongoose.model(
    '关联-高考库报名录取信息表',
    studentSchema,
    '关联-高考库报名录取信息表'
  );

  let connectSchema = new mongoose.Schema({
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
    录取日期: String,
    证书情况: Array,
    转移历史: Array,
    单位编号: String,
    姓名: String,
    文化程度: String,
    进入年份: String,
    退出年份: String,
    转出地域: String,
    证书等级: String,
    '国家职业资格等级（工人技术等级）]': String,
    专业技术职务: String,
    '行政职务(级别)': String,
    社保所在地区: String
  });

  let connectForm = mongoose.model(
    '关联库-高考库关联社保库',
    connectSchema,
    '关联库-高考库关联社保库'
  );

  let count = 1;
  const cursor = studentForm.find({}).cursor();
  let doc;
  while ((doc = await cursor.next())) {
    const form = await ssForm.findOne({ 公民身份号码: doc.身份证号 });
    if (form) {
      const person = {
        考生号: doc.考生号,
        考生姓名: doc.考生姓名,
        身份证号: doc.身份证号,
        性别代码: doc.性别代码,
        出生日期: doc.出生日期,
        毕业类别代码: doc.毕业类别代码,
        户籍地代码: doc.户籍地代码,
        录取院校代码: doc.录取院校代码,
        录取院校名称: doc.录取院校名称,
        录取专业名称: doc.录取专业名称,
        录取层次代码: doc.录取层次代码,
        录取标准代码: doc.录取标准代码,
        录取批次代码: doc.录取批次代码,
        录取日期: doc.录取日期,
        证书情况: form.证书情况,
        转移历史: form.转移历史,
        单位编号: form.单位编号,
        姓名: form.姓名,
        文化程度: form.文化程度,
        进入年份: form.进入年份,
        退出年份: form.退出年份,
        转出地域: form.转出地域,
        证书等级: form.证书等级,
        '国家职业资格等级（工人技术等级）]':
          form['国家职业资格等级（工人技术等级）]'],
        专业技术职务: form.专业技术职务,
        '行政职务(级别)': form['行政职务(级别)'],
        社保所在地区: form.社保所在地区
      };
      const psn = new connectForm(person);
      await psn.save();
    } else {
      console.log(`${count++}`);
      continue;
    }
    console.log(`${count++}`);
  }

  mongoose.disconnect();
}

query();
