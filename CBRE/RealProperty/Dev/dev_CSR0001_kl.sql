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
		    THEN DATEDIFF(month, cost_tran_recur_date_amort_start, cost_tran_recur_date_amort_end) + 1
	        ELSE 12
	    END AS termMonths

    FROM CbreStaging.archibus_cost_tran_recur
    ),

AmortCostData AS
(SELECT
        cost_tran_recur_ls_id,
        cost_tran_recur_cost_cat_id,
        cost_tran_recur_amount_income_base_payment,
        CASE
            WHEN cost_tran_recur_date_amort_start IS NOT NULL
            THEN CAST(cost_tran_recur_date_amort_start AS DATE)
            ELSE CAST(cost_tran_recur_date_start AS DATE)
        END AS AmortStart,

        CASE
            WHEN cost_tran_recur_date_amort_end IS NOT NULL
            THEN CAST(cost_tran_recur_date_amort_end AS DATE)

            ELSE DATEADD(
                DAY,
                -1,
                DATEADD(
                    YEAR,
                    1,
                    CASE
                        WHEN MONTH(cost_tran_recur_date_start) >= 4
                        THEN DATEFROMPARTS(YEAR(cost_tran_recur_date_start),4,1)
                        ELSE DATEFROMPARTS(YEAR(cost_tran_recur_date_start)-1,4,1)
                    END
                )
            )
        END AS AmortEnd

    FROM CbreStaging.archibus_cost_tran_recur

    WHERE cost_tran_recur_cost_cat_id LIKE '%AMORT%' AND cost_tran_recur_period = 'MONTH'
),

AmortSeed AS
(
    SELECT
        cost_tran_recur_ls_id,
        cost_tran_recur_cost_cat_id,
        cost_tran_recur_amount_income_base_payment,
        AmortStart,
        AmortEnd,

        CASE
            WHEN MONTH(AmortStart) >= 4
                THEN DATEFROMPARTS(YEAR(AmortStart),4,1)
            ELSE
                DATEFROMPARTS(YEAR(AmortStart)-1,4,1)
        END AS FYStart

    FROM AmortCostData
),

AmortizationSplit AS
(
    SELECT
        cost_tran_recur_ls_id,
        cost_tran_recur_cost_cat_id,
        cost_tran_recur_amount_income_base_payment,
        AmortStart,
        AmortEnd,
        FYStart

    FROM AmortSeed
    UNION ALL

    SELECT
        cost_tran_recur_ls_id,
        cost_tran_recur_cost_cat_id,
        cost_tran_recur_amount_income_base_payment,
        AmortStart,
        AmortEnd,
        DATEADD(YEAR,1,FYStart)

    FROM AmortizationSplit

    WHERE DATEADD(YEAR,1,FYStart) <= AmortEnd
),

AmortizationFY AS
(
    SELECT
        cost_tran_recur_ls_id,
        cost_tran_recur_cost_cat_id,
        cost_tran_recur_amount_income_base_payment,

        CONCAT(
            RIGHT(YEAR(FYStart),2),
            RIGHT(YEAR(DATEADD(YEAR,1,FYStart)),2)
        ) AS FY,

        FYStart,

        DATEADD(
            DAY,
            -1,
            DATEADD(YEAR,1,FYStart)
        ) AS FYEnd,

        CASE
            WHEN AmortStart > FYStart
            THEN AmortStart
            ELSE FYStart
        END AS OverlapStart,

        CASE
            WHEN AmortEnd <
                 DATEADD(DAY,-1,DATEADD(YEAR,1,FYStart))
            THEN AmortEnd
            ELSE DATEADD(DAY,-1,DATEADD(YEAR,1,FYStart))
        END AS OverlapEnd

    FROM AmortizationSplit
),

AmortTotals AS
(
    SELECT
        cost_tran_recur_ls_id,
        FY,

        SUM(
            cost_tran_recur_amount_income_base_payment *
            (
                DATEDIFF(
                    MONTH,
                    OverlapStart,
                    OverlapEnd
                ) + 1
            )
        ) AS Amort

    FROM AmortizationFY

    GROUP BY
        cost_tran_recur_ls_id,
        FY
),

BPIndData AS
(
    SELECT
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
        cost_tran_recur_resp_type,
        cost_tran_recur_parking_stalls

    FROM CbreStaging.archibus_cost_tran_recur
),

BPAggData AS
(
    SELECT
        cost_tran_recur_ls_id,
        FY,

        SUM(
            CASE
                WHEN (
                        cost_tran_recur_cost_cat_id LIKE '%PRKG%'
                     OR cost_tran_recur_cost_cat_id LIKE '%PARKING%'
                     )
                     AND cost_tran_recur_resp_type <> 'LE'
                THEN cost_tran_recur_parking_stalls
                ELSE 0
            END
        ) AS BillableParkingStalls

    FROM BPIndData

    GROUP BY
        cost_tran_recur_ls_id,
        FY
),

CalcCosts AS
    (SELECT
        cost_tran_recur_ls_id,
        FY,
        /*MAX(
            CASE
                WHEN cost_tran_recur_cost_cat_id <> 'PARKING'
                THEN NULLIF(cost_tran_recur_area,0)
                END)
            AS BillableAreaBuilding,*/
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
                WHEN cost_tran_recur_cost_cat_id IN ('PARKING','PARKING ADMIN_BA')
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
    WHERE rmpct_rm_cat LIKE '%PRKG%' OR rmpct_rm_cat LIKE '%PARKING%'
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
    am.ls_area_negotiated - am.ls_appropriated_sqm AS BillableAreaBuilding,
	am.TotalRentableLand AS AreaLand,
    ROUND(am.TotalRentableLand,2) AS AppropriatedAreaLand,
    am.TotalRentableLand - ROUND(am.TotalRentableLand, 2) AS BillableAreaLand,
    tp.TotalParking AS TotalParkingStalls,
    am.ls_appropriated_parking_stalls AS AppropriatedAreaParking,
    IIF(bp.BillableParkingStalls < 0, 0, bp.BillableParkingStalls) AS BillableParkingStalls,
    cc.BaseRent AS BaseRent,
    cc.OM AS OM,
    cc.Utilities AS Utilities,
    cc.OM + cc.Utilities AS OMTotal,
    cc.LLOM AS LLOM,
    cc.Tax AS Tax,
    ISNULL(at.Amort,0) AS Amort,    
    cc.ParkingCost AS ParkingCost,
    cc.LLAdmin as LLAdmin,
    cc.OMAdmin AS OMAdmin,
    cc.UtilAdmin AS UtilAdmin,
    cc.TaxAdmin AS TaxAdmin,
    cc.LLAdmin + cc.OMAdmin + cc.UtilAdmin + cc.TaxAdmin AS TotalAdmin,
    cc.BaseRent + cc.OM + cc.Utilities + cc.LLOM + cc.Tax + ISNULL(at.Amort,0) + cc.ParkingCost + cc.LLAdmin + cc.OMAdmin + cc.UtilAdmin + cc.TaxAdmin AS AnnualCharge
FROM AgreementMaster am


LEFT JOIN CalcCosts cc ON am.ls_ls_id = cc.cost_tran_recur_ls_id
LEFT JOIN AmortTotals at
    ON am.ls_ls_id = at.cost_tran_recur_ls_id
   AND cc.FY = at.FY
LEFT JOIN BPAggData bp
    ON am.ls_ls_id = bp.cost_tran_recur_ls_id
   AND cc.FY = bp.FY
LEFT JOIN TotalParking tp ON am.ls_ls_id = tp.rmpct_ls_id

WHERE cc.FY IN ('2526', '2627')

ORDER BY AgreementNum,
         FY