
with listing_returns as 
(
select
	al.listing_id
	,al.top_category
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
	`etsy-data-warehouse-prod`.etsy_shard.seller_star_seller_status ss 
on 
	al.shop_id = ss.shop_id
left join 
	`etsy-data-warehouse-prod`.incrementals.listing_daily ld -- Proxy for Return policy suggestion pre-migration
on
	al.listing_id = ld.listing_id and ld.date = "2022-09-18" 
where 
	al.is_digital = 0)

------------------Suggested vs Migrated return policy---------
select
	return_policy_suggested
	,return_policy,
	count(*) as listings
from 
	listing_returns
group by 1,2
order by 1,2;

---------breakdown by start seller---------
select
	is_star_seller 
	,sum(case when return_policy = "Accepts Returns" then 1 else 0 end) as Accepts_Returns
	,sum(case when return_policy = "Does Not Accepts Returns" then 1 else 0 end) as Does_Not_Accepts_Returns
	,sum(case when return_policy = "No Policy" then 1 else 0 end) as No_Policy
from 
	listing_returns
group by
	is_star_seller;

---------breakdown by Seller Tiers---------
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

----------breakdown by Core Markets-------
select
	Core_Markets 
	,sum(case when return_policy = "Accepts Returns" then 1 else 0 end) as Accepts_Returns
	,sum(case when return_policy = "Does Not Accepts Returns" then 1 else 0 end) as Does_Not_Accepts_Returns
	,sum(case when return_policy = "No Policy" then 1 else 0 end) as No_Policy
from 
	listing_returns
group by
	Core_Markets;


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

