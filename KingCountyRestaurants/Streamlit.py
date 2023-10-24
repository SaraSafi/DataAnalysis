import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import plotly
from datetime import datetime
import plotly.express as px
from PIL import Image
################################################################################
st.header('Food_Establishment_Inspection_Data')
st.write(''' داده ی نهایی''')
df1=pd.read_csv('Clean_Data.csv')
df1['City']=df1['City'].apply(lambda x: x.lower())
df1 = df1.loc[:, ~df1.columns.str.contains('^Unnamed')]
data1 = pd.read_csv('new.csv',sep=',')
data1 = data1.loc[:, ~data1.columns.str.contains('^Unnamed')]
st.write(data1)
st.write(''' همانطور که در نمودار زیر مشخص است, ستون های:
Violation point و Inspection score
Grade , Inspection score
Grade , Violation points
به ترتیب بیشترین همبستگی را با هم دارند.''')
image = Image.open('1.png')
st.image(image)
st.write(''' ************************************************************************************************''')

image = Image.open('2.png')
st.image(image)
st.write(''' ************************************************************************************************''')
st.write('''در ابتدا به بررسی جامعه اماری پرداخته ایم که این دیتاست شامل چه تعداد شهر میباشد و هر شهر دارای چه تعداد رستوران میباشد و هر رستوران دارای چند شعبه ی زنجیره ای میباشد و همچنین هر شهر دارای چه تعداد رستوران درجه یک و .. میباشد و ایا این که این رستوران های زنجیره ای بیشتر دارای کدام درجه میباشند و در مرحله بعد به بررسی تعداد بازدید ها و بررسی علت و امتیاز تخلفات پرداخته ایم و این که ایا تخلفات منجر به بسته شدن رستوران ها گردیده است یا خیر و ایا تعطیلی رستوران ها به علت بحران جهانی بوده است''')
st.write('''در این دیتا ست بهداشت 53 شهر مورد بررسی قرار گرفته است ''')
st.write(''' شهرهای مورد بررسی قرار گرفته شده در نقشه زیر نمایش داده شده است''')
image = Image.open('3.png')
st.image(image)
st.write(''' ************************************************************************************************''')
st.write(''' حدود 48 در صد اطلاعات مربوط به دیتاست مربوط به شهر سیاتل است به عبارتی 48 درصد رستوران ها در شهر سیاتل واقع شده اند چون شهر سیاتل یکی از بزرگترین شهر ها در دیتاست میباشد''')
Count_num=np.round(df1['City'].value_counts(normalize=True)*100)
Count_num.to_frame().head()
Count_num =pd.DataFrame({'City':Count_num.index, 'Number':Count_num.values})
st.write(Count_num )
st.write(''' ************************************************************************************************''')
st.write('''و حدود 4346 رستوران در شهر سیاتل واقع شده است ''')
count =df1.groupby(['City'])['Name'].nunique().sort_values(ascending=False)
c=count.to_frame()
st.write(c)
st.write(''' ************************************************************************************************''')
st.write(''' نمایش فراوانی رستوران ها نمودار زیر''')
image = Image.open('4.png')
st.image(image)
st.write(''' ************************************************************************************************''')
st.write(''' شهرهای مورد بررسی قرار گرفته شده در نقشه زیر نمایش داده شده است''')
image = Image.open('5.png')
st.image(image)
st.write(''' ************************************************************************************************''')
st.write(''' نمودار فراوانی شهرها بدون شهر سیاتل''')
image = Image.open('26.png')
st.image(image)
st.write(''' ************************************************************************************************''')
st.write(''' نمودار فراوانی شهرها بدون شهر سیاتل''')
image = Image.open('27.png')
st.image(image)
st.write(''' ************************************************************************************************''')
st.write(''' در اینجا رستورانهای زنجیره ای در سطح شهرهای مختلف را پیدا کردیم و همانطور که واضح است شهر سیاتل دارای بیشترین رستوران های زنجیره ای میباشد که بیش از 80 درصد این شعبات درجه یک هستند 
و نمودار 15 تا از رستورانهای زنجیره ای برتر را نمایش داده ایم''')
st.write(''' حدود 24 درصد رستوران ها زنجیره ای هستند''')
image = Image.open('6.png')
st.image(image)
st.write(''' ************************************************************************************************''')
count =df1.groupby(['Name','City'])['Business_ID'].nunique().sort_values(ascending=False)
d=count.to_frame()
st.write(d)
count =df1.groupby(['Name','Grade'])['Business_ID'].nunique().sort_values(ascending=False)
w=count.to_frame()
st.write(w)

st.write(''' ************************************************************************************************''')
image = Image.open('7.png')
st.image(image)
st.write(''' ************************************************************************************************''')
df_pivot = pd.pivot_table(
    data1,
    values='Business_ID',
    index="City", 
    columns='Grade',
    aggfunc='nunique'
)
ax = df_pivot.plot(kind="bar")
fig1 = ax.get_figure()
fig1.set_size_inches(17, 6)
ax.set_xlabel("Days")
ax.set_ylabel("Show the grade of Chain restaurants")
st.pyplot(fig1)
st.write(''' ************************************************************************************************''')
image = Image.open('50.png')
st.image(image)
st.write(''' ************************************************************************************************''')
st.write(''' ************************************************************************************************''')
st.write(''' نمایش شهرهای دارای رستوران زنجیره ای رو نقشه''')
image = Image.open('28.png')
st.image(image)
image = Image.open('29.png')
st.image(image)

st.write(''' ************************************************************************************************''')
st.write('''همانطور که از نمودار زیر مشخص است  میانگین تعداد رستوران های زنجیره ای در سال 2020 به شدت کاهش پیدا کرده است و در زیر به بررسی علت این کاهش میپردازیم ''')
df_pivot = pd.pivot_table(
    data1,
    values="Chain_res",
    index="Year",
    columns="Grade",
    aggfunc='mean'
)
ax = df_pivot.plot(kind="line")
fig2 = ax.get_figure()
fig2.set_size_inches(17, 6)
ax.set_xlabel("Years")
ax.set_ylabel("Average chain resturant")
ax.legend(bbox_to_anchor=(1.0, 1.0))
st.pyplot(fig2)
st.write(''' ************************************************************************************************''')
st.write(''' و همانطور که از نمودار زیر مشخص است تعداد رستوران ها در سال 2020 به شدت کاهش پیدا کرده است و در مراحل زیر میخواهیم بررسی کنیم ایا این تعطیلی ها ناشی از موارد تخلفات است یا ناشی از بحران جهانی کرونا''')
image = Image.open('8.png')
st.image(image)
st.write(''' ************************************************************************************************''')
st.write(''' ************************************************************************************************''')
st.write(''' ************************************************************************************************''')
st.write(''' در این مرحله به بررسی تعداد بازرسی ها و تعداد تخلفات و بررسی رابطه امتیاز ها و نوع تخلفات و بسته یا باز بودن رستوران پرداخته ایم''')
st.write(''' جدول زیر نمایشگر تعداد بازدید های صورت گرفته و تعداد تخلفات که منجر به دریافت کارت شده است از هر رستوران بر اساس رستوران های زنجیره ای و سال ها میباشد''')
st.write(''' ''')
pivot =np.round(pd.pivot_table(data1, values=('Chain_res','Date_ins_year','violation_total','Year','IS_chain'), 
                        index=['Name','City']).sort_values(by=['Date_ins_year'], ascending=False).fillna(0))
st.write(pivot)
st.write(''' ************************************************************************************************''')
st.write(''' و همچنین بررسی میکنیم ایا بازه ی زمانی روی تعداد تخلفات تاثیر داشته است یا خیر؟''')
st.write(''' همانطور که واضح تعداد بازدیدها و تعداد تخلفات در سالهای 2020 و 2021 و2022 تقریبا نصف سالهای 2006 تا 2015 میباشد و دلیل این موضوع کاهش به شدت زیاد تعداد رستوران ها در سال 2020 میباشد  ''')
df_pivot = pd.pivot_table(
    data1,
    values=("Date_ins_year",'violation_total'),
    index="Year",    
)
ax = df_pivot.plot(kind="bar")
fig3 = ax.get_figure()
fig3.set_size_inches(17, 6)
ax.set_xlabel("Years")
ax.set_ylabel("Sum of Inspection and violation")
st.pyplot(fig3)
st.write(''' ************************************************************************************************''')
st.write(''' در این قسمت به بررسی درجه رستوران ها میپردازیم همانطور که مشخص است رستورانهای درجه 4 بیشترین میزان تخلفات را داشته و به طبع آن میزان بازرسی های بیشتری را به خودش اختصاص داده است.''')
df_pivot = pd.pivot_table(
    data1,
    values=("Date_ins_year",'violation_total'),
    index="Grade",    
)
ax = df_pivot.plot(kind="bar")
fig4 = ax.get_figure()
fig4.set_size_inches(17, 6)
ax.set_xlabel("Years")
ax.set_ylabel("Sum of Inspection and violation")
st.pyplot(fig4)
st.write(''' ************************************************************************************************''')
pivot1 = np.round(pd.pivot_table(data1, values=('Date_ins_year','violation_total','Year','Month'), 
                        index=['City','Grade']).sort_values(by=['Date_ins_year'], ascending=False).fillna(0))
st.write(pivot1)
st.write(''' ************************************************************************************************''')
st.write(''' در نمودار زیر به بررسی تعداد تخلفات در روزهای هفته پرداخته ایم و همانطور که واضح است روز یکشنبه که اخر هفته است و رستورانها شلوغ تر هستند موارد تخلف بیشتری رخ داده است اما به علت تعطیلات اخر هفته تعداد بازرسی های بیشتری صورت نگرفته است''')
df_pivot = pd.pivot_table(
    data1,
    values=("Date_ins_year",'violation_total'),
    index="day_of_week",    
)
ax = df_pivot.plot(kind="bar")
fig5 = ax.get_figure()
fig5.set_size_inches(17, 6)
ax.set_xlabel("Days")
ax.set_ylabel("Sum of Inspection and violation")
st.pyplot(fig5)
st.write(''' ************************************************************************************************''')
st.write(''' همانطور که از نمودار زیر مشخص است بیشترین کارت های تخلف مربوط به رستورانهای زنجیره ایی در اخر هفته ها میباشد اما تعداد بازدید ها در اخر هفته ها کاهش پیدا کرده است''')
df_pivot = pd.pivot_table(
    data1,
    values=("Date_ins_year",'violation_total'),
    columns='IS_chain',
    index="day_of_week",    
)
ax = df_pivot.plot(kind="bar")
fig6 = ax.get_figure()
fig6.set_size_inches(17, 6)
ax.set_xlabel("Days")
ax.set_ylabel("Sum of Inspection and violation")
st.pyplot(fig6)
st.write(''' ************************************************************************************************''')
st.write(''' در جدول زیر بررسی شده است کدام رستوران در کدام شهر بیشترین میانگین امتیاز بازرسی ها را دارد''')
st.write(''' همانطور که واضح است بیشترین میانگین امتیازات بازرسی مربوط به رستوران های درجه سه میباشد''')
df_mean1=np.round(data1.groupby(['City','Name','IS_chain']).agg({'Inspection_Score':'mean'})).sort_values(by=['Inspection_Score'], ascending=False)
df_mean=df_mean1.groupby(level=1).head(1)
st.write(df_mean)
st.write(''' ************************************************************************************************''')
st.write(''' نمودار میانگین امتیاز بازرسی ها بر اساس شهر و درجه رستوران ها''')
df_pivot = pd.pivot_table(
    df1,
    values="Inspection_Score",
    index="City",
    columns='Grade',
    aggfunc='mean'
    
)
ax = df_pivot.plot(kind="bar")
fig7 = ax.get_figure()
fig7.set_size_inches(17, 6)
ax.set_xlabel("City")
ax.set_ylabel("Average Inspection_Score according Grade")
ax.legend(bbox_to_anchor=(1.0, 1.0))
st.pyplot(fig7)
st.write(''' ************************************************************************************************''')
st.write(''' نمایش درجه ی رستوران ها بر روی نقشه''')
image = Image.open('30.png')
st.image(image)
image = Image.open('31.png')
st.image(image)
image = Image.open('32.png')
st.image(image)
image = Image.open('33.png')
st.image(image)

st.write(''' ************************************************************************************************''')
st.write(''' بررسی امتیاز بازرسی بر اساس شهر ها و زنجیره ای بودن رستوران های شهر
میتوان از نمودار زیر نتیجه گرفت بیشتر رستوران های غیر زنجیره ای در تمامی شهرها بیشترین میانگین امتیاز تخلفات را دریافت کرده اند ''')
df_pivot = pd.pivot_table(
    data1,
    values="Inspection_Score",
    index="City",
    columns='IS_chain',
    aggfunc='mean'
    
)
ax = df_pivot.plot(kind="bar")
fig8 = ax.get_figure()
fig8.set_size_inches(17, 6)
ax.set_xlabel("City")
ax.set_ylabel("Average Violation points according IS_chain")
ax.legend(bbox_to_anchor=(1.0, 1.0))
st.pyplot(fig8)
st.write(''' ************************************************************************************************''')
st.write(''' از نمودار زیر میتوان نتیجه گرفت بیشتر امتیاز های بازرسی مربوط به رستوران های غیر زنجیره ای در سال های 2014 و 2015 و 2016 است''')
df_pivot = pd.pivot_table(
    data1,
    values="Inspection_Score",
    index="Year",
    columns='IS_chain',
    aggfunc='mean'
    
)
ax = df_pivot.plot(kind="bar")
fig9 = ax.get_figure()
fig9.set_size_inches(17, 6)
ax.set_xlabel("Years")
ax.set_ylabel("Average Inspection_Score according IS_chain")
ax.legend(bbox_to_anchor=(1.0, 1.0))
st.pyplot(fig9)
st.write(''' ************************************************************************************************''')
image = Image.open('9.png')
st.image(image)
st.write(''' ************************************************************************************************''')
st.write('''  بیشترین امتیاز های تخلفات ''')
df_mean1=np.round(data1.groupby(['City','Grade','IS_chain']).agg({'Violation_Points':'mean'})).sort_values(by=['Violation_Points'], ascending=False)
df_mean2=df_mean1.groupby(level=1).head(1)
st.write(df_mean2)
st.write(''' ************************************************************************************************''')
df_pivot = pd.pivot_table(
    df1,
    values="Violation_Points",
    index="City",
    columns='Grade',
    aggfunc='mean'
    
)
ax = df_pivot.plot(kind="bar")
fig10= ax.get_figure()
fig10.set_size_inches(17, 6)
ax.set_xlabel("City")
ax.set_ylabel("Average Violation points according Grade")
ax.legend(bbox_to_anchor=(1.0, 1.0))
st.pyplot(fig10)
st.write(''' ************************************************************************************************''')
st.write(''' از نمودار زیر میتوان نتیجه گرفت که رستوران های زنجیره ای شهر سیاتل که بیشترین رستوران های زنجیره ای را دارد امتیاز تخلفات قابل توجهی دارند''')
df_pivot = pd.pivot_table(
    data1,
    values="Violation_Points",
    index="City",
    columns='IS_chain',
    aggfunc='mean'
    
)
ax = df_pivot.plot(kind="bar")
fig11 = ax.get_figure()
fig11.set_size_inches(17, 6)
ax.set_xlabel("City")
ax.set_ylabel("Average Violation points according IS_chain")
ax.legend(bbox_to_anchor=(1.0, 1.0))
st.pyplot(fig11)
st.write(''' ************************************************************************************************''')
df_mean1=np.round(df1.groupby(['Grade','Year']).agg({'Inspection_Score':'mean','Violation_Points':'mean'})).sort_values(by=['Violation_Points'], ascending=False)
df_mean3=df_mean1.groupby(level=1).head(1)
st.write(df_mean3)
st.write(''' ************************************************************************************************''')
st.write(''' همانطور که واضح است بیشترین امتیاز تخلفات رستوران ها مربوط به سالهای 2007 و2014 و 2020 میباشد که در انها رستوران های درجه 4 بیشترین امتیاز تخلفات را دریافت کرده اند''')
#بررسی میانگین تخلفات رستوران ها بر اساس درجه ان ها
df_pivot = pd.pivot_table(
    df1,
    values="Violation_Points",
    index="Year",
    columns="Grade",
    aggfunc='mean'
    
)
ax = df_pivot.plot(kind="bar")
fig12 = ax.get_figure()
fig12.set_size_inches(17, 6)
ax.set_xlabel("Years")
ax.set_ylabel("Average Violation points according Grade")
ax.legend(bbox_to_anchor=(1.0, 1.0))
st.pyplot(fig12)
st.write(''' ************************************************************************************************''')
st.write(''' همانطور که از این نمودار و نمودار قبلی مشخص است بیشتر امتیاز تخلفات در سال های مختلف مربوط به رستوران های غیر زنجیره ایی میباشد و این امر بسیار بدیهی است چون تعداد رستوران های زنجیره ای در برابر رستوران های غیر زنجیره ای بسیار کمتر است و این که بیشتر رستوران های زنجیره ای درجه یک و دو هستند و این رستوران ها کمترین میزان امتیازات بازرسی نسبت به رستوران های درجه سه و چهار دارند''')
df_pivot = pd.pivot_table(
    data1,
    values="Violation_Points",
    index="Year",
    columns="IS_chain",
    aggfunc='mean'
    
)
ax = df_pivot.plot(kind="bar")
fig13 = ax.get_figure()
fig13.set_size_inches(17, 6)
ax.set_xlabel("Years")
ax.set_ylabel("Average Violation points according Chain")
ax.legend(bbox_to_anchor=(1.0, 1.0))
st.pyplot(fig13)
st.write(''' ************************************************************************************************''')
st.write(''' از نمودار زیر میتوان نتیجه گرفت که میانگین امتیاز های تخلفات رستوران ها در بهار و تابستان سال 2020 به شدت کاهش پیدا کرده است و علت این موضوع کاهش چشمگیر رستوران ها در این سال میباشد در ادامه به بررسی این کاهش میپردازیم''')
df_pivot = pd.pivot_table(
    df1,
    values="Violation_Points",
    index="Year",
    columns="Season",
    aggfunc='mean'
)
ax = df_pivot.plot(kind="line")
fig14 = ax.get_figure()
fig14.set_size_inches(17, 6)
ax.set_xlabel("Years")
ax.set_ylabel("Average Violation points in seasons")
ax.legend(bbox_to_anchor=(1.0, 1.0))
st.pyplot(fig14)
st.write(''' ************************************************************************************************''')
st.write('''نمودار زیر نشان میدهد که بیشتر رستوران های زنجیره ای و هم غیر زنجیره ای از لحاظ امتیاز تخلفات کاهش چشمگیری در سال 2020 نسبت به پیک خود در سال 2015 داشته اند و دلیل آن کاهش تعداد رستوران ها میباشدد''')
df_pivot = pd.pivot_table(
    data1,
    values="Violation_Points",
    index="Year",
    columns="IS_chain",
    aggfunc='mean'
)
ax = df_pivot.plot(kind="line")
fig15 = ax.get_figure()
fig15.set_size_inches(17, 6)
ax.set_xlabel("Years")
ax.set_ylabel("Average Violation points")
ax.legend(bbox_to_anchor=(1.0, 1.0))
st.pyplot(fig15)
st.write(''' ************************************************************************************************''')
st.write(''' ************************************************************************************************''')
st.write(''' ************************************************************************************************''')
st.write(''' در این مرحله به بررسی علل تخلفات رستوران هایی که بیشترین امتیازها را دریافت کرده اند میپردازیم''')
st.write(''' بررسی توزیع فراوانی هر کارت''')
image = Image.open('11.png')
st.image(image)
image = Image.open('12.png')
st.image(image)
st.write(''' ************************************************************************************************''')
st.write(''' در این جا به این موضوع پرداختیم که کدام نوع تخلفات مشمول دریافت کارت ابی و قرمز و یا سبز گشته اند
همانطور که مشخص است کارت ابی مربوط به بهداشت عمومی و ضد عفونی کردن محیط ظروف و رو میزی ها و ... پرداخته است
و کارت قرمز بیشتر به دمای نگهداری مواد غذایی و سلامت کارکنان و رعایت بهداشت فردی کارکنان پرداخته است''')
CrosstabResult1=pd.crosstab(index=df1['Violation_Description'],columns=df1['Violation_Type']).sort_values(by = ['BLUE'], ascending = [False])
st.write(CrosstabResult1)
image = Image.open('24.png')
st.image(image)
CrosstabResult2=pd.crosstab(index=df1['Violation_Description'],columns=df1['Violation_Type']).sort_values(by = ['RED'], ascending = [False])
st.write(CrosstabResult2)
image = Image.open('23.png')
st.image(image)
CrosstabResult3=pd.crosstab(index=df1['Violation_Description'],columns=df1['Violation_Type']).sort_values(by = ['Green'], ascending = [False])
st.write(CrosstabResult3)
image = Image.open('25.png')
st.image(image)
st.write(''' ************************************************************************************************''')
st.write(''' در نمودار زیر مشخص گردیده است که رستوران هایی با درجه 3 و4 در گروه ریسک پذیری 3 بیشترین امتیاز تخلفات را به خود اختصاص داده اند''')
df_pivot = pd.pivot_table(
    df1,
    values=("Violation_Points"),
    index="Description",
    columns='Grade',
    
    aggfunc='mean'    
)
ax = df_pivot.plot(kind="bar",stacked=True,)
fig16 = ax.get_figure()
fig16.set_size_inches(17, 6)
ax.set_xlabel("Years")
ax.set_ylabel("Average Violation points")
ax.legend(bbox_to_anchor=(1.0, 1.0))
st.pyplot(fig16)
st.write(''' ************************************************************************************************''')
st.write(''' نمودار زیر نشان میدهد کدام گروه از ریسک پذبری دارای بیشترین تعداد کارت تخلفات است''')
st.write(''' همانطور که از نمودار پایین مشخص است بیشترین تعداد کارتهای دریافتی توسط رستوران هایی با گروه ریسک پذیری 3 میباشد''')
df_pivot = pd.pivot_table(
    df1,
    values=("Violation_Points"),
    index="Description",
    columns='Violation_Type',
    
    aggfunc='count'    
)
ax = df_pivot.plot(kind="bar",stacked=True,)
fig17 = ax.get_figure()
fig17.set_size_inches(17, 6)
ax.set_xlabel("Years")
ax.set_ylabel("Average Violation points")
ax.legend(bbox_to_anchor=(1.0, 1.0))
st.pyplot(fig17)
st.write(''' ************************************************************************************************''')
st.write(''' نمودار زیر به بررسی رستوران های زنجیره ای و نوع ریسک پذیری بر اساس میانگین امتیاز های تخلفات میردازد''')
df_pivot = pd.pivot_table(
    data1,
    values=("Violation_Points"),
    index="Description",
    columns='IS_chain',
    
    aggfunc='count'    
)
ax = df_pivot.plot(kind="bar",stacked=True,)
fig18 = ax.get_figure()
fig18.set_size_inches(17, 6)
ax.set_xlabel("Years")
ax.set_ylabel("Average Violation points")
ax.legend(bbox_to_anchor=(1.0, 1.0))
st.pyplot(fig18)
st.write(''' ************************************************************************************************''')
st.write(''' همانطور که مشخص است شهر سیاتل با بیشترین رستوران های درجه یک و بیشترین رستوران های زنجیره ای دارای بیشترین کارت تخلف قرمز را دریافت کرده است و بیشترین رستوران های بسته شده بر اساس تخلفات را دارد''')
CrosstabResult4=pd.crosstab(index=df1['City'],columns=df1['Violation_Type']).sort_values(by = ['RED'], ascending = [False])
st.write(CrosstabResult4)

st.write(''' ************************************************************************************************''')
df_mean1=np.round(df1.groupby(['City']).agg({'Violation_Points':'mean','Violation_Type':'count'})).sort_values(by=['Violation_Type'], ascending=False)
st.write(df_mean3)
image = Image.open('35.png')
st.image(image)
st.write(''' ************************************************************************************************''')
st.write(''' بدون شهر سیاتل''')
df_mean3=np.round(df1.groupby(['City']).agg({'Violation_Points':'mean','Violation_Type':'count'})).sort_values(by=['Violation_Type'], ascending=False)
df_mean3.drop(['seattle'], axis = 0, inplace =True)
st.write(df_mean3)
image = Image.open('36.png')
st.image(image)

st.write(''' ************************************************************************************************''')
st.write(''' شهر سیاتل بیشترین تعداد تخلفات و بیشترین تعداد رستوران های بسته شده را دارد و بیشترین کارت های دریافت شده''')
CrosstabResult5=pd.crosstab(index=df1['City'],columns=df1['Inspection_Closed_Business'])
st.write(CrosstabResult5)
st.write(''' بیشتر تعطیلی ها مربوط به رستوران درجه 2 میباشد''')
image = Image.open('14.png')
st.image(image)
st.write(''' ************************************************************************************************''')
df_pivot = pd.pivot_table(
    df1,
    values=("Violation_Points"),
    index="City",
    columns=('Inspection_Closed_Business','Grade'),
    
    aggfunc='count'    
)
ax = df_pivot.plot(kind="bar",stacked=True,)
fig19 = ax.get_figure()
fig19.set_size_inches(17, 6)
ax.set_xlabel("Years")
ax.set_ylabel("Average Violation points")
ax.legend(bbox_to_anchor=(1.0, 1.0))
st.pyplot(fig19)
st.write(''' ************************************************************************************************''')
image = Image.open('51.png')
st.image(image)
st.write(''' ************************************************************************************************''')
st.write(''' از این دو نمودار میتوان نتیجه گرفت تعطیلی های سال 2020 به دلیل تخلفات نبوده و بحران جهانی کرونا باعث تعطیلی رستوران ها گشته است''')
image = Image.open('15.png')
st.image(image)
st.write(''' ************************************************************************************************''')
st.write(''' بر اساس هر کارت چه تعداد رستوران بسته شده یا باز است''')
image = Image.open('16.png')
st.image(image)
st.write(''' ************************************************************************************************''')
image = Image.open('17.png')
st.image(image)
st.write(''' ************************************************************************************************''')
st.write(''' حدود 116 رستوران زنجیره ای بسته شده اند یعنی حدود 16 درصد از رستوران های بسته شده زنجیره ای هستند و نمدار مربوطه در پایین رسم شده است''')
image = Image.open('18.png')
st.image(image)
st.write(''' ************************************************************************************************''')
df_pivot = pd.pivot_table(
    data1,
    values=("Violation_Points"),
    index="City",
    columns=('Inspection_Closed_Business',"IS_chain"),
    
    aggfunc='count'    
)
ax = df_pivot.plot(kind="bar",stacked=True,)
fig20 = ax.get_figure()
fig20.set_size_inches(17, 6)
ax.set_xlabel("Years")
ax.set_ylabel("Average Violation points")
ax.legend(bbox_to_anchor=(1.0, 1.0))
st.pyplot(fig20)
st.write(''' ************************************************************************************************''')
image = Image.open('52.png')
st.image(image)
st.write(''' ************************************************************************************************''')
image = Image.open('20.png')
st.image(image)
st.write(''' ************************************************************************************************''')
st.write(''' همانطور که از نمودار زیر مشخص است شهر سیاتل با بیشترین امتیاز تخلف و بیشترین کارت های دریافتی تخلف و دارای بیشترین تعداد رستوران زنجیره ای بیشترین تعداد رستوران های بسته شده را دارد''')
image = Image.open('22.png')
st.image(image)

st.write(''' ************************************************************************************************''')
st.write(''' در اوردن شهرها با رستورانهایی با کیفیت بیشتر''')
st.write(''' 
همانطور که از نمودارهای زیر پیداست رستورانی با کیفیت است که نمره بازرسی کمتری دریافت کرده باشد
و همچنین باید نمره تخلف کمتری دریافت کرده باشد
و همانطور که مشخص است در گروه بندی انجام شده زستوران هایی که در گروه رضایت و کامل قرار گرفته اند دارای بیشترین کیفیت و کمترین نارضایتی هستند و گروه ناراضی کمترین کیفیت و گروهای دیگر هم شامل اطلاعات ناقص بوده اند بنابراین نمیتوان راجع به انها نظر قاطعی داد.''')
stock=data1.groupby('Inspection_Result')['Inspection_Score'].mean()
fig20 = px.bar(stock, x=stock.index, y=stock.values,title = 'Violation points according to resturant',labels={'y':'Inspection score'})
st.plotly_chart(fig20, use_container_width=True)
stock=data1.groupby('Inspection_Result')['Violation_Points'].mean()
fig21 = px.bar(stock, x=stock.index, y=stock.values,title = 'Violation points according to resturant',labels={'y':'Inspection score'})
st.plotly_chart(fig21, use_container_width=True)
st.write(''' بر اساس نمودار زیر با کیفیت ترین رستوران ها در سال های 2008 و 2020 و 2021 با میانگین امتیاز 11 قرار دارند''')
stock=data1.groupby('Year')['Inspection_Score'].mean()
fig22 = px.bar(stock, x=stock.index, y=stock.values,title = 'Violation points according to resturant',labels={'y':'Inspection score'})
st.plotly_chart(fig22, use_container_width=True)

st.write('''در این مرحله رستوران ها را با توجه به امتیاز های بازرسی دسته بندی کردیم و نوع ریسک پذیری آنهارا مشخص کردیم''')
st.write('''با توجه به نمودار فوق از 0 تا 5 (که قبلا نیز تمیز حساب شده بود) درجه تمیز 
از 5 تا چارک 3ام (20) آلوده 
از 20 تا چارک 4ام (50) خطرناک 
و بالای 50 را بسیار خطرناک و مرگآور میخوانیم''')
fig80= px.box(data1,x="Inspection_Score")
st.plotly_chart(fig80, use_container_width=True)
st.write(''' ************************************************************************************************''')
q3_1 = data1[["City","Name","Inspection_Score"]]
q3_1["City"] = q3_1["City"].apply(func=lambda x:x.upper())
def score_category(inspection_score:int):
    if 0 <= inspection_score < 5:
        return "Safe"
    elif 5 <= inspection_score < 20:
        return "Infected"
    elif 20<=inspection_score < 50:
        return "Dangerous"
    elif 50<=inspection_score:
        return "Fatal"
q3_2 = pd.DataFrame(q3_1.groupby(by=["City","Name"],group_keys=True).mean())
q3_2["Category"] = q3_2["Inspection_Score"].apply(lambda x:score_category(x))
q3_2.reset_index(drop=False,inplace=True)
st.write(q3_2)
st.write(''' ************************************************************************************************''')
q3_3 = pd.DataFrame(q3_2.value_counts(["City"])).reset_index(drop=False)
q3_3.rename(columns={"City":"City",0:"Count"},inplace=True)
q3_3=q3_3[q3_3["Count"]>=5]
q3_final = pd.merge(q3_2,q3_3,how="inner",on="City")
st.write(q3_final)
fig_q3 = px.histogram(q3_final,x="City",color="Category",barmode='group',opacity=1)
st.plotly_chart(fig_q3, use_container_width=True)