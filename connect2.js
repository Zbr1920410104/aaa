const mongoose = require('mongoose');

async function query() {
  await mongoose.connect(
    'mongodb://keanuo:19980411@server-ubuntu:27017/talent?authSource=talent',
    {
      useNewUrlParser: true,
      useUnifiedTopology: true
    }
  );

  let tal1Schema = mongoose.Schema({
    身份证号: String,
    性别: String,
    户籍地代码: String,
    录取院校代码: String,
    录取院校名称: String,
    录取专业名称: String,
    录取层次代码: String,
    录取标准代码: String,
    录取批次代码: String,
    录取年份: String,
    证书情况: Array,
    转移历史: Array,
    单位编号: String,
    姓名: String,
    文化程度: String,
    进入年份: String,
    退出年份: String,
    转出地域: String,
    证书等级: String,
    国家职业资格证书: String,
    专业技术职务: String,
    行政职务: String,
    社保所在地区: String
  });

  let tal1Form = mongoose.model(
    '(简化)高考库关联社保库-省内',
    tal1Schema,
    '(简化)高考库关联社保库-省内'
  );

  let companySchema = mongoose.Schema({
    单位编号: String,
    地址: String,
    单位类型: String,
    行业代码: String,
    隶属关系: String
  });

  let companyForm = mongoose.model(
    '社保库-城镇-单位信息表',
    companySchema,
    '社保库-城镇-单位信息表'
  );

  let tal1NewSchema = new mongoose.Schema({
    身份证号: String,
    性别: String,
    户籍地代码: String,
    录取院校代码: String,
    录取院校名称: String,
    录取专业名称: String,
    录取层次代码: String,
    录取标准代码: String,
    录取批次代码: String,
    录取年份: String,
    证书情况: Array,
    转移历史: Array,
    单位编号: String,
    姓名: String,
    文化程度: String,
    进入年份: String,
    退出年份: String,
    转出地域: String,
    证书等级: String,
    国家职业资格证书: String,
    专业技术职务: String,
    行政职务: String,
    社保所在地区: String,
    地址: String,
    单位类型: String,
    行业代码: String,
    隶属关系: String
  });

  let tal1NewForm = mongoose.model(
    '高考库关联社保个人库单位库-省内生源',
    tal1NewSchema,
    '高考库关联社保个人库单位库-省内生源'
  );

  let count = 1;
  const cursor = tal1Form.find({}).cursor();
  let doc;
  while ((doc = await cursor.next())) {
    const form = await companyForm.findOne({ 单位编号: doc.单位编号 });
    const person = {
      身份证号: doc.身份证号,
      性别: doc.性别,
      户籍地代码: doc.户籍地代码,
      录取院校代码: doc.录取院校代码,
      录取院校名称: doc.录取院校名称,
      录取专业名称: doc.录取专业名称,
      录取层次代码: doc.录取层次代码,
      录取标准代码: doc.录取标准代码,
      录取批次代码: doc.录取批次代码,
      录取年份: doc.录取年份,
      证书情况: doc.证书情况,
      转移历史: doc.转移历史,
      单位编号: doc.单位编号,
      姓名: doc.姓名,
      文化程度: doc.文化程度,
      进入年份: doc.进入年份,
      退出年份: doc.退出年份,
      转出地域: doc.转出地域,
      证书等级: doc.证书等级,
      国家职业资格证书: doc.国家职业资格证书,
      专业技术职务: doc.专业技术职务,
      行政职务: doc.行政职务,
      社保所在地区: doc.社保所在地区,
      地址: form ? form.地址 : null,
      单位类型: form ? form.单位类型 : null,
      行业代码: form ? form.行业代码 : null,
      隶属关系: form ? form.隶属关系 : null
    };
    const psn = new tal1NewForm(person);
    await psn.save();
    console.log(`${count++}`);
  }

  mongoose.disconnect();
}
query();
