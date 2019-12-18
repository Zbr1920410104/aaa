const mongoose = require('mongoose');

async function query() {
  await mongoose.connect(
    'mongodb://keanuo:19980411@localhost:27017/talent?authSource=talent',
    {
      useNewUrlParser: true,
      useUnifiedTopology: true
    }
  );

  let foreSchema = mongoose.Schema({
    个人编号: String,
    '国家职业资格等级（工人技术等级）]': String,
    专业技术职务: String,
    '行政职务(级别)': String,
    社保所在地区: String
  });

  let foreForm = mongoose.model(
    '社保库-城镇-个人信息表',
    foreSchema,
    '社保库-城镇-个人信息表'
  );

  let afterSchema = mongoose.Schema({
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

  let afterForm = mongoose.model(
    '新关联库-城镇-个人信息汇总全集表',
    afterSchema,
    '新关联库-城镇-个人信息汇总全集表'
  );

  let newSchema = new mongoose.Schema({
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

  let newForm = mongoose.model(
    'new1-社保库个人信息表',
    newSchema,
    'new1-社保库个人信息表'
  );

  let count = 1;
  const cursor = afterForm.find({}).cursor();
  let doc;
  while ((doc = await cursor.next())) {
    const form = await foreForm.findOne({ 个人编号: doc.个人编号 });
    if (form) {
      const person = {
        个人编号: doc.个人编号,
        证书情况: doc.证书情况,
        转移历史: doc.转移历史,
        单位编号: doc.单位编号,
        姓名: doc.姓名,
        性别: doc.性别,
        公民身份号码: doc.公民身份号码,
        文化程度: doc.文化程度,
        进入年份: doc.进入年份,
        退出年份: doc.退出年份,
        转出地域: doc.转出地域,
        证书等级: doc.证书等级,
        '国家职业资格等级（工人技术等级）]': form['国家职业资格等级（工人技术等级）]'],
        专业技术职务: form.专业技术职务,
        '行政职务(级别)': form['行政职务(级别)'],
        社保所在地区: form.社保所在地区
      };
      const psn = new newForm(person);
      await psn.save();
    } else continue;
    console.log(`${count++}`);
  }

  mongoose.disconnect();
}

query();
