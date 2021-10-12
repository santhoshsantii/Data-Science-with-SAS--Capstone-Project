/* SAS Capstone Project */
/* Importing the Datasets */
%let path =/home/u59461173/Assignments;

proc import datafile="&path/orders.csv" out=orders dbms=csv replace;
run;

proc print data=orders(obs=10);
run;

proc contents data=orders;
run;

libname cntry xlsx "&path/country_lookup.xlsx";

proc print data=cntry.countries(obs=10);
run;

proc contents data=cntry.countries;
run;

/* Validating the data */
proc freq data=cntry.countries order=freq;
	tables country_key country_name;

proc print data=cntry.countries;
	where country_key in ('AG', 'CF', 'GB', 'US');
	var country_key country_name;

proc sort data=cntry.countries out=country_clean dupout=dups nodupkey;
	by country_key;

	/* Cleaning datasets */
	/* Orders Frequency Analysis:  */
/* Find out and put your analysis in excel sheet */
ods pdf file="&path/Anomalies.pdf";

proc print data=orders;
	title 'Orders with invalid date';
	where order_date > delivery_date;
run;

proc freq data=orders;
	title 'Invalid order type';
	table order_type;

proc freq data=orders;
	title 'Invalid case of the country key';
	table Customer_Country;
	ods pdf close;

proc freq data=orders;
	table Customer_Continent;

proc means data=orders;
	var quantity retail_price cost_price;

proc print data=orders;
	where quantity < 0;

/* Solving the Issues: */
/* Solving Delivery date issue */
data data orders_clean1;
	set orders;
	where delivery_date >=order_date;
run;

proc print data=orders_clean1;
	where order_date > delivery_date;

/* Solving Order Type Issue */
data orders_clean2(drop=order_type);
	set orders_clean1;
	length Order_type_det $10;

	select(order_type);
		when (1) Order_type_det='Retail';
		when (2) Order_type_det='Phone';
		when (3) Order_type_det='Internet';
		otherwise Order_type_det='Invalid';
	end;
run;

proc freq data=orders_clean2;
	table Order_type_det;

/*Solving Country key case issue */
data orders_clean3;
	set orders_clean2;
	Customer_Country=upcase(Customer_Country);
run;

proc freq data=orders_clean3;
	table customer_country;

data orders_clean4;
	set orders_clean3;

	if quantity < 0 then
		quantity=0;
run;

/* Creating Age Group */
proc format;
	value AgeGroup LOW-18='<=18' 19-25='BW 19 and 25' 26-40='BW 26 and 40' 
		41 - 60='BW 41-60' 61-HIGH='>=61' other='Unknown';

/* Creating Shipping Days, Profit, Age Group to Use it in problem later */
data orders_clean5(drop=age customer_dob retail_price cost_price quantity);
	set orders_clean4;
	profit=(retail_price - cost_price) * quantity;
	shipping_days=intck('day', Order_Date, Delivery_Date);
	age=intck('year', Customer_Dob, today());
	age_group=put(age, AgeGroup.);
run;

proc sort data=country_clean;
	by country_key;

proc sort data=orders_clean5;
	by customer_country;

/* Merging the datasets */
data final_clean(drop=lat lon);
	merge country_clean(in=in1 rename=(country_key=customer_country)) 
		orders_clean5(in=in2);
	by customer_country;

	if in1=1 and in2=1;
run;

/* Which months have the highest and lowest total number of orders? */
proc freq data=final_clean order=freq;
	table order_date/nocum;
	format order_date monname.;

/* How many orders are distributed by each continent? */
proc freq data=final_clean order=freq;
	table Customer_Continent/nocum;

/* Within each continent how many orders were placed via retail, internet or phone? */
proc freq data=final_clean order=freq;
	table Customer_Continent*Order_type_det/norow nocol nopercent;

	/* Ship Days Summary:  */
/* How Many days on average does it take for an order to be delivered? */
proc means data=final_clean mean maxdec=2;
	var shipping_days;
	where shipping_days > 0;

/* Are there any countries where shipment takes longer? */
proc means data=final_clean mean maxdec=2;
	var shipping_days;
	class country_name;
	where shipping_days > 0;

	/* Profit Analysis by Customer Age:  */
/* Which customer age group produces the highest median profit per order  */
proc means data=final_clean median maxdec=2 noprint nway;
	var profit;
	class age_group;
	output out=profit_summ median=medprof;