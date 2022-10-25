
with listing_returns as 
(
select
	al.listing_id
	,l.shop_id
	,al.top_category
	,case when date(timestamp_seconds(l.original_create_date)) >= "2022-09-22" then 1 else 0 end as created_post_migration
	,case 
		when al.accepts_returns = 1 then "Accepts Returns"
		when al.accepts_returns = 0 then "Does Not Accepts Returns"
		when al.accepts_returns is null then "No Policy"
		else "Other" end as return_policy
	,case 
		when ld.accepts_returns = 1 then "Accepts Returns"
		when ld.accepts_returns = 0 then "Does Not Accepts Returns"
		when ld.accepts_returns is null then "No Policy"
		else "Other" end as return_policy_suggested
	,al.accepts_exchanges
	,case 
		when al.origin_country_name in ("United States", "Canada", "United Kingdom", "France", "Germany", "Australia", "India") then al.origin_country_name
		else "Rest of World" end as Core_Markets
	,al.seller_tier_new as Seller_Tier
	,ss.is_star_seller
from 
	`etsy-data-warehouse-prod`.rollups.active_listing_shipping_costs al
left join 
	`etsy-data-warehouse-prod`.listing_mart.listings l
on 
	al.listing_id = l.listing_id
left join 
	`etsy-data-warehouse-prod`.etsy_shard.seller_star_seller_status ss 
on 
	al.shop_id = ss.shop_id
left join 
	`etsy-data-warehouse-prod`.incrementals.listing_daily ld -- Proxy for Return policy suggestion pre-migration
on
	al.listing_id = ld.listing_id and 
	ld.date = "2022-09-18" 
where 
	al.is_digital = 0),

shop_level_returns as 
(select
	shop_id
	,sum(case when return_policy = "Accepts Returns" then 1 else 0 end)/count(*) as accepts_returns_share
	,sum(case when return_policy = "Does Not Accepts Returns" then 1 else 0 end)/count(*) as doesnot_accepts_returns_share
	,sum(case when return_policy = "No Policy" then 1 else 0 end)/count(*) as no_policy_share
from 
	listing_returns
group by
	shop_id),

listing_views_agg as
(select
	listing_id
	,count(visit_id) as listing_views
from 
	`etsy-data-warehouse-prod`.analytics.listing_views
where 
	_date between DATE_SUB(current_date(), INTERVAL 7 DAY) and current_date() -- Change date range accordingly
group by 
	listing_id)

----------breakdown by top category------------
select
	top_category 
	,sum(case when return_policy = "Accepts Returns" then 1 else 0 end) as Accepts_Returns
	,sum(case when return_policy = "Does Not Accepts Returns" then 1 else 0 end) as Does_Not_Accepts_Returns
	,sum(case when return_policy = "No Policy" then 1 else 0 end) as No_Policy
from 
	listing_returns
group by
	top_category;

-- ----------breakdown by Core Markets-------
select
	Core_Markets 
	,sum(case when return_policy = "Accepts Returns" then 1 else 0 end) as Accepts_Returns
	,sum(case when return_policy = "Does Not Accepts Returns" then 1 else 0 end) as Does_Not_Accepts_Returns
	,sum(case when return_policy = "No Policy" then 1 else 0 end) as No_Policy
from 
	listing_returns
group by
	Core_Markets;

-- ---------breakdown by Seller Tiers---------
select
	Seller_Tier 
	,sum(case when return_policy = "Accepts Returns" then 1 else 0 end) as Accepts_Returns
	,sum(case when return_policy = "Does Not Accepts Returns" then 1 else 0 end) as Does_Not_Accepts_Returns
	,sum(case when return_policy = "No Policy" then 1 else 0 end) as No_Policy
from 
	listing_returns
where
	Seller_Tier is not null
group by
	Seller_Tier;

-- ---------breakdown by star seller---------
select
	is_star_seller 
	,sum(case when return_policy = "Accepts Returns" then 1 else 0 end) as Accepts_Returns
	,sum(case when return_policy = "Does Not Accepts Returns" then 1 else 0 end) as Does_Not_Accepts_Returns
	,sum(case when return_policy = "No Policy" then 1 else 0 end) as No_Policy
from 
	listing_returns
group by
	is_star_seller;

-- ------------------Suggested vs Migrated return policy---------
select
	return_policy_suggested
	,return_policy
	,count(*) as listings
from 
	listing_returns
group by 1,2
order by 1,2;

-- ------------------ breakdown by listing creation date---------

select
	created_post_migration
	,return_policy
	,count(*) as listings
from 
	listing_returns
group by
	created_post_migration
	,return_policy
order by 1,2;

-----------Shops with and without return policies-------

select
	count(*) as total_sellers
	,sum(case when no_policy_share = 1 then 1 else 0 end) as sellers_w_no_policy
	,sum(case when accepts_returns_share + doesnot_accepts_returns_share > 0 then 1 else 0 end) as sellers_w_return_policy_partial
	,sum(case when accepts_returns_share + doesnot_accepts_returns_share = 1 then 1 else 0 end) as sellers_w_return_policy_all
from 
	shop_level_returns;

----------- Listing views breakdown--------
select 
	sum(case when lr.return_policy in ("Accepts Returns", "Does Not Accepts Returns") then lv.listing_views else 0 end)/sum(lv.listing_views) as views_w_return_policy_share
	,sum(case when lr.return_policy = "Accepts Returns" then lv.listing_views else 0 end)/sum(lv.listing_views) as views_w_accepts_returns_share
	,sum(case when lr.return_policy = "No Policy" then lv.listing_views else 0 end)/sum(lv.listing_views) as views_w_no_policy_share
from 
	listing_returns lr 
inner join 
	listing_views_agg lv 
on 
	lr.listing_id = lv.listing_id
