const mongoose = require('mongoose');

async function simplify1() {
  await mongoose.connect(
    'mongodb://keanuo:19980411@server-ubuntu:27017/talent?authSource=talent',
    {
      useNewUrlParser: true,
      useUnifiedTopology: true
    }
  );

  let connectSchema = mongoose.Schema({
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

  let tal1Schema = new mongoose.Schema({
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

  let tal2Schema = new mongoose.Schema({
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

  let tal2Form = mongoose.model(
    '(简化)高考库关联社保库-省外',
    tal2Schema,
    '(简化)高考库关联社保库-省外'
  );

  const college = [
    '10213',
    '10217',
    '10225',
    '10224',
    '10212',
    '10222',
    '10232',
    '10214',
    '10223',
    '10226',
    '10228',
    '10231',
    '10240',
    '10220',
    '10234',
    '11802',
    '10219',
    '10229',
    '11230',
    '10235',
    '10233',
    '10242',
    '10236',
    '13744',
    '10245',
    '11445',
    '14560',
    '13304',
    '13298',
    '11446',
    '13303',
    '13299',
    '13306',
    '13301',
    '11635',
    '13300',
    '12729',
    '13296',
    '13307',
    '14017',
    '10238',
    '12905',
    '14095',
    '11449',
    '12728',
    '12724',
    '12726',
    '12718',
    '10872',
    '11046',
    '12053',
    '12725',
    '12727',
    '13935',
    '13918',
    '14053',
    '12911',
    '12906',
    '12907',
    '12910',
    '14178',
    '12490',
    '14108',
    '13448',
    '13453',
    '13449',
    '13447',
    '12908',
    '13732',
    '13450',
    '13729',
    '13731',
    '14272',
    '14425',
    '14540',
    '14055',
    '13451',
    '13730',
    '14400',
    '14633',
    '13302'
  ];

  const cursor = connectForm.find({}).cursor();
  let doc;
  let count = 1;
  while ((doc = await cursor.next())) {
    console.log(`${count++}`);
    let test = 0;
    for (let num = 0; num < 81; num++) {
      if (doc.录取院校代码 == college[num]) {
        test = 1;
      }
    }
    const transDoc = simplifyOneDoc(doc);
    const sf = /黑龙江|鹤岗|齐齐哈尔|牡丹江|佳木斯|大庆|鸡西|双鸭山|伊春|七台河|鹤岗|黑河|绥化|兴安岭/;
    if (test | sf.test(transDoc.录取院校名称)) {
      const ti = new tal1Form(transDoc);
      await ti.save();
    } else {
      const ti = new tal2Form(transDoc);
      await ti.save();
    }
  }
  console.log('finished');

  mongoose.disconnect();
}

simplify1();

function simplifyOneDoc(doc) {
  let 身份证号 = doc.身份证号;
  let 性别 = doc.性别代码 == '1' ? '男' : '女';
  let 户籍地代码 = doc.户籍地代码;
  let 录取院校代码 = doc.录取院校代码;
  let 录取院校名称 = doc.录取院校名称;
  let 录取专业名称 = doc.录取专业名称;
  let 录取层次代码 = doc.录取层次代码;
  let 录取标准代码 = doc.录取标准代码;
  let 录取批次代码 = doc.录取批次代码;
  let date = doc.录取日期;
  let 录取年份 = date.substring(0, 4);
  let 证书情况 = doc.证书情况;
  let 转移历史 = doc.转移历史;
  let 单位编号 = doc.单位编号;
  let 姓名 = doc.姓名;
  let 文化程度 = doc.文化程度;
  let 进入年份 = doc.进入年份;
  let 退出年份 = doc.退出年份;
  let 转出地域 = doc.转出地域;
  let 证书等级 = doc.证书等级;
  let 国家职业资格证书 = doc['国家职业资格等级（工人技术等级）]'];
  let 专业技术职务 = doc.专业技术职务;
  let 行政职务 = doc['行政职务(级别)'];
  let 社保所在地区 = doc.社保所在地区;

  return {
    身份证号,
    性别,
    户籍地代码,
    录取院校代码,
    录取院校名称,
    录取专业名称,
    录取层次代码,
    录取标准代码,
    录取批次代码,
    录取年份,
    证书情况,
    转移历史,
    单位编号,
    姓名,
    文化程度,
    进入年份,
    退出年份,
    转出地域,
    证书等级,
    国家职业资格证书,
    专业技术职务,
    行政职务,
    社保所在地区
  };
}
