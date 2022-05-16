--****PLEASE ENTER YOUR DETAILS BELOW****
--Q2-mau-queries.sql
--Student ID:31314295
--Student Name:JINGYI CHEN
--Tutorial No:01

/* Comments for your marker:

For gallery id status table, due to the T and G state made it clear that need gallery id, so that when the state is G and T, 
we need to insert the gallery id field, but other state gallery id is null, because whether it's in the warehouse or sold, 
or is returned to the author, can assume that time have nothing to do with gallery, so there is no gallery id


*/


/*
2(i) Query 1
*/
--PLEASE PLACE REQUIRED SQL STATEMENT FOR THIS PART HERE

select * from artist;

select 
	artist_code as "Artist Code",
case 
	when artist_fname is null then artist_gname
	when artist_gname is null then artist_fname
else artist_fname || ' ' || artist_gname
	end as "Artist Name",
case when artist_state = 'VIC' then 'Victoria'
	when artist_state = 'NSW' then 'New South Wales'
	when artist_state = 'WA' then 'Western Australia'
	end as "Artist State"
from artist
where 
	artist_state in ('VIC','NSC','WA')
	and (artist_gname is null
		or artist_fname is null
		or artist_phone is null)
order by "Artist Name",artist_code;



/*
2(ii) Query 2
*/
--PLEASE PLACE REQUIRED SQL STATEMENT FOR THIS PART HERE


select 
	t.artist_code as "Artist Code", 
	artist_fname || ' ' || artist_gname as "Artist Name",
	w.artwork_no as "Artwork No.",
	w.artwork_title as "Artwork Title",
	to_char(w.artwork_minpayment,'$99999999999.99')as "Artwork Min.Payment",
	trunc(aws_date_time - artwork_submitdate) as "Number of Days with MAU"
from 
	AW_STATUS s 
		join Artwork w on s.artwork_no = w.artwork_no and s.artist_code = w.artist_code
		join artist t on t.artist_code = s.artist_code
where 
	aws_action = 'R' 
		and 
	trunc(aws_date_time - artwork_submitdate) < 120
		and
	gallery_id is null
order by
	t.artist_code asc,
	trunc(aws_date_time - artwork_submitdate) desc

/*
2(iii) Query 3
*/
--PLEASE PLACE REQUIRED SQL STATEMENT FOR THIS PART HERE
select 
    d.artist_code as "Artist Code",
    d.artwork_no as "Artwork No.",
    w.artwork_title as "Artwork Title",
    d.gallery_id as "Gallery ID",
    g.gallery_name as "Gallery Name",
    to_char(aw_display_start_date,'Dy dd Mon    yyyy') as "Display Start Date",
    aw_display_end_date - aw_display_start_date as "Number of Days in Gallery"
from 
    AW_DISPLAY d join artwork w on d.artwork_no = w.artwork_no and d.artist_code = w.artist_code
    join gallery g on d.gallery_id = g.gallery_id
where aw_display_end_date - aw_display_start_date < 13
order by d.artist_code,d.artwork_no,aw_display_end_date - aw_display_start_date,d.gallery_id,to_char(aw_display_start_date,'Dy dd Mon    yyyy');



/*
2(iv) Query 4
*/
--PLEASE PLACE REQUIRED SQL STATEMENT FOR THIS PART HERE

select 
	s.artist_code as "Artist Code",
	s.artwork_no as "Artwork No.",
	w.artwork_title as "Artwork Title",
	count(s.aws_action) as "Number of Movements"
from AW_STATUS s join artwork w on s.artwork_no = w.artwork_no and s.artist_code = w.artist_code
where 
	aws_action = 'T' 
group by 
	s.artist_code,
	s.artwork_no,
	w.artwork_title
having count(s.aws_action) < 
	(select avg(count(aws_action))
		from 
			AW_STATUS
		where 
			aws_action = 'T'
		group by 
			artist_code,
			artwork_no)
order by 
	count(s.aws_action) asc,
	s.artist_code asc,
	s.artwork_no asc; 




/*
2(v) Query 5
*/
--PLEASE PLACE REQUIRED SQL STATEMENT FOR THIS PART HERE

select distinct
    w.artist_code,
    w.artwork_title,
    to_char((w.artwork_minpayment / (1-0.2-0.01*(select gallery_sale_percent from gallery WHERE gallery_id = 1))),'$999999999') as "Min. Sale Price Est. (Gallery 1)",
    to_char((w.artwork_minpayment / (1-0.2-0.01*(select gallery_sale_percent from gallery WHERE gallery_id = 2))),'$999999999') as "Min. Sale Price Est. (Gallery 2)",
    to_char((w.artwork_minpayment / (1-0.2-0.01*(select gallery_sale_percent from gallery WHERE gallery_id = 3))),'$999999999') as "Min. Sale Price Est. (Gallery 3)",
    to_char((w.artwork_minpayment / (1-0.2-0.01*(select gallery_sale_percent from gallery WHERE gallery_id = 4))),'$999999999') as "Min. Sale Price Est. (Gallery 4)",
    to_char((w.artwork_minpayment / (1-0.2-0.01*(select gallery_sale_percent from gallery WHERE gallery_id = 5))),'$999999999') as "Min. Sale Price Est. (Gallery 5)"
from AW_STATUS s 
    join artwork w on s.artwork_no = w.artwork_no and s.artist_code = w.artist_code
where s.aws_action = 'G' 
order by w.artist_code,w.artwork_title;
/*
2(vi) Query 6
*/
--PLEASE PLACE REQUIRED SQL STATEMENT FOR THIS PART HERE
select 
	to_char(d.artist_code) "Artist Code", 
	r.artist_fname || ' ' || r.artist_gname as "Artist Full Name",
	w.artwork_title "Artwork Title",
	to_char(d.gallery_id) as "Gallery ID",
    to_char(s.sale_price,'$99999999999.99')as "Sale Price",
	to_char(((s.sale_price - w.artwork_minpayment)/w.artwork_minpayment)*100,'999999D99') ||'%' as "% Sold Above Min. Sell Price"
from AW_DISPLAY d
	join artwork w 
on d.artwork_no = w.artwork_no and d.artist_code = w.artist_code 
	join sale s 
on s.aw_display_id  = d.aw_display_id
	join artist r
on r.artist_code = d.artist_code
union all
select 
	'-------------' as "Artist Code",
	'--------------------' as "Artist Full Name",
	'----------------' as "Artwork Title",
	'-------------' as "Gallery ID", 
	'Average: ' as "Sale Price", 
	round (sum(round(100*((sale_price - artwork_minpayment)/artwork_minpayment),1))/ count(*),1) || '%' as "% Sold Above Min.SellPrice" 
from artist natural join artwork natural join aw_display natural join sale;





