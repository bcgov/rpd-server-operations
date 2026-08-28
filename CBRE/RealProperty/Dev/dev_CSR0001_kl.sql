WITH AgreementMaster AS
	(SELECT
		ls.ls_ls_id,
		ls.ls_bl_id,
		ls.ls_pr_id,
		ls.ls_status,
		ls.ls_ls_parent_id,
		ls.ls_fasb_ls_type,
		FORMAT(ls.ls_date_end, 'yyyy-MM-dd') AS ls_date_end,
        ls.ls_area_negotiated,
		ls.ls_appropriated_sqm,
		ls.ls_appropriated_parking_stalls,
		FORMAT(parent.ls_date_start, 'yyyy-MM-dd') AS ParentStart,
		FORMAT(parent.ls_date_end, 'yyyy-MM-dd') AS ParentEnd,
		bl.linkAddress,
		bl.linkCity,
		BranchClient.dp_name AS bcDepName,
		BranchClient.Client AS bcClient,
		pr.TotalRentableLand
	FROM CbreStaging.archibus_ls ls

	LEFT JOIN CbreStaging.archibus_bl bl
		ON ls.ls_bl_id = bl.BuildingId

	LEFT JOIN (
		SELECT
			dp.dp_dp_id,
			dp.dp_name,
			MAX(dv.dv_name) AS Client
		FROM CbreStaging.archibus_dp dp
		LEFT JOIN CbreStaging.archibus_dv dv
			ON dp.dp_dv_id = dv.dv_dv_id
		GROUP BY
			dp.dp_dp_id,
			dp.dp_name
			) BranchClient ON ls.ls_tn_name = BranchClient.dp_dp_id

	LEFT JOIN CbreStaging.archibus_property pr
		ON ls.ls_pr_id = pr.PropertyId

	LEFT JOIN CbreStaging.archibus_ls parent 
		ON ls.ls_ls_parent_id = parent.ls_ls_id

	WHERE ls.ls_status IN ('ACTIVE','DRAFT')),

CostData AS
    (SELECT
        cost_tran_recur_ls_id,
        CONCAT(
            RIGHT(
                CASE
                    WHEN MONTH(cost_tran_recur_date_start) >= 4
                    THEN YEAR(cost_tran_recur_date_start)
                    ELSE YEAR(cost_tran_recur_date_start)-1
                END,
                2
            ),
            RIGHT(
                CASE
                    WHEN MONTH(cost_tran_recur_date_start) >= 4
                    THEN YEAR(cost_tran_recur_date_start)+1
                    ELSE YEAR(cost_tran_recur_date_start)
                END,
                2
            )
        ) AS FY,
        cost_tran_recur_cost_cat_id,
        cost_tran_recur_amount_income_base_payment,
        cost_tran_recur_period,
        cost_tran_recur_date_amort_start,
        cost_tran_recur_date_amort_end,
        cost_tran_recur_area,
        cost_tran_recur_parking_stalls,
        CASE
	        WHEN cost_tran_recur_date_amort_start IS NOT NULL 
		    THEN DATEDIFF(month, cost_tran_recur_date_amort_start, cost_tran_recur_date_amort_end) 
	        ELSE 12
	    END AS termMonths

    FROM CbreStaging.archibus_cost_tran_recur
    ),

CalcCosts AS
    (SELECT
        cost_tran_recur_ls_id,
        FY,
        AVG(
            CASE
                WHEN cost_tran_recur_cost_cat_id <> 'PARKING'
                THEN NULLIF(cost_tran_recur_area,0)
                END)
            AS BillableAreaBuilding,
        MAX(cost_tran_recur_parking_stalls) AS BillableParkingStalls,
        SUM(
            CASE
                WHEN cost_tran_recur_cost_cat_id = 'BASE RENT'
                    AND cost_tran_recur_period = 'MONTH'
                THEN cost_tran_recur_amount_income_base_payment * termMonths
                ELSE 0
            END
        ) AS BaseRent,
        SUM(
            CASE
                WHEN cost_tran_recur_cost_cat_id = 'OPERATIONS & MAINTENANCE'
                    AND cost_tran_recur_period = 'MONTH'
                THEN cost_tran_recur_amount_income_base_payment * termMonths
                ELSE 0
            END
        ) AS OM,
            SUM(
            CASE
                WHEN cost_tran_recur_cost_cat_id = 'UTILITIES'
                    AND cost_tran_recur_period = 'MONTH'
                THEN cost_tran_recur_amount_income_base_payment * termMonths
                ELSE 0
            END
        ) AS Utilities,
        SUM(
            CASE
                WHEN cost_tran_recur_cost_cat_id = 'LANDLORD PROVIDED O&M'
                    AND cost_tran_recur_period = 'MONTH'
                THEN cost_tran_recur_amount_income_base_payment * termMonths
                ELSE 0
            END
        ) AS LLOM,
        SUM(
            CASE
                WHEN cost_tran_recur_cost_cat_id = 'PROPERTY TAX'
                    AND cost_tran_recur_period = 'MONTH'
                THEN cost_tran_recur_amount_income_base_payment * termMonths
                ELSE 0
            END
        ) AS Tax,
        SUM(
            CASE
                WHEN cost_tran_recur_cost_cat_id = '%AMORTIZATION%'
                    AND cost_tran_recur_period = 'MONTH'
                THEN cost_tran_recur_amount_income_base_payment * termMonths
                ELSE 0
            END
        ) AS Amort,
        SUM(
            CASE
                WHEN cost_tran_recur_cost_cat_id = 'PARKING'
                    AND cost_tran_recur_period = 'MONTH'
                THEN cost_tran_recur_amount_income_base_payment * termMonths
                ELSE 0
            END
        ) AS ParkingCost,
        SUM(
            CASE
                WHEN cost_tran_recur_cost_cat_id = 'LEASE ADMINISTRATION FEE'
                    AND cost_tran_recur_period = 'MONTH'
                THEN cost_tran_recur_amount_income_base_payment * termMonths
                ELSE 0
            END
        ) AS LLAdmin,
        SUM(
            CASE
                WHEN cost_tran_recur_cost_cat_id = 'O&M ADMIN'
                    AND cost_tran_recur_period = 'MONTH'
                THEN cost_tran_recur_amount_income_base_payment * termMonths
                ELSE 0
            END
        ) AS OMAdmin,
        SUM(
            CASE
                WHEN cost_tran_recur_cost_cat_id = 'UTILITY ADMIN'
                    AND cost_tran_recur_period = 'MONTH'
                THEN cost_tran_recur_amount_income_base_payment * termMonths
                ELSE 0
            END
        ) AS UtilAdmin,
        SUM(
            CASE
                WHEN cost_tran_recur_cost_cat_id = 'TAX ADMIN'
                    AND cost_tran_recur_period = 'MONTH'
                THEN cost_tran_recur_amount_income_base_payment * termMonths
                ELSE 0
            END
        ) AS TaxAdmin
    FROM CostData
    GROUP BY
    cost_tran_recur_ls_id,
    FY
    ),

TotalParking AS
    (SELECT 
        rmpct_ls_id,
	    COUNT(DISTINCT rmpct_rm_id) AS TotalParking
    FROM CbreStaging.archibus_rmpct
    WHERE rmpct_rm_cat LIKE '%PRKG%'
    GROUP BY
    rmpct_ls_id
    )

SELECT 
    am.bcClient AS Client,
    am.bcDepName AS Branch,
    am.linkCity AS City,
    CASE
        WHEN am.ls_bl_id IS NOT NULL 
        THEN am.ls_bl_id
        ELSE am.ls_pr_id
        END AS 'Location',
    am.linkAddress AS 'Address',
    am.ls_ls_parent_id AS Lease,
    am.ls_ls_id AS AgreementNum,
    am.ls_status AS AgrStatus,
    cc.FY AS FY,
	am.ls_fasb_ls_type AS AgreementType,
	am.ParentStart AS LeaseStart,
	am.ParentEnd AS LeaseExpiry,
	am.ls_date_end AS AgrDurationEnd,
    am.ls_area_negotiated AS RentableAreaBuilding,
	am.ls_appropriated_sqm AS AppropriatedAreaBuilding,
    cc.BillableAreaBuilding AS BillableAreaBuilding,
	am.TotalRentableLand AS AreaLand,
    ROUND(am.TotalRentableLand, 2) AS AppropriatedAreaLand,
    am.TotalRentableLand - ROUND(am.TotalRentableLand, 2) AS BillableAreaLand,
    tp.TotalParking AS TotalParkingStalls,
    am.ls_appropriated_parking_stalls AS AppropriatedAreaParking,
    cc.BillableParkingStalls AS BillableParkingStalls,
    cc.BaseRent AS BaseRent,
    cc.OM AS OM,
    cc.Utilities AS Utilities,
    cc.OM + cc.Utilities AS OMTotal,
    cc.LLOM AS LLOM,
    cc.Tax AS Tax,
    cc.Amort AS Amort,
    cc.ParkingCost AS ParkingCost,
    cc.LLAdmin as LLAdmin,
    cc.OMAdmin AS OMAdmin,
    cc.UtilAdmin AS UtilAdmin,
    cc.TaxAdmin AS TaxAdmin,
    cc.LLAdmin + cc.OMAdmin + cc.UtilAdmin + cc.TaxAdmin AS TotalAdmin,
    cc.BaseRent + cc.OM + cc.Utilities + cc.LLOM + cc.Tax + cc.Amort + cc.ParkingCost + cc.LLAdmin + cc.OMAdmin + cc.UtilAdmin + cc.TaxAdmin AS AnnualCharge
FROM AgreementMaster am

LEFT JOIN CalcCosts cc ON am.ls_ls_id = cc.cost_tran_recur_ls_id
LEFT JOIN TotalParking tp ON am.ls_ls_id = tp.rmpct_ls_id

--WHERE ls_ls_id = 'A5125290-L5728'

ORDER BY AgreementNum