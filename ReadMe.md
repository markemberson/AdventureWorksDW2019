# AdventureWorksDW 2019 ETL

A SSIS package was originally created for AdventureWorks 2008, having found this on the CodePlex archive site https://archive.codeplex.com/?p=msftisprodsamples, i've upgraded it to run in Visual Studio 2019. A diagram of the ETL flow is included in this repository.

Below is a list of the tables in AdventureWorksDW 2019, with the source query as taken from the 2008 SSIS package.

# Target Tables

## dbo.AdventureWorksDWBuildVersion

Bulk Insert from AdventureWorksDWBuildVersion.csv

## dbo.DatabaseLog

## dbo.DimAccount

Bulk Insert from DimAccount.csv

## dbo.DimCurrency

~~~~SQL
SELECT 
CurrencyCode AS CurrencyAlternateKey, 
Name AS CurrencyName
FROM Sales.Currency;
~~~~

## dbo.DimCustomer

~~~~SQL
SELECT 
    cu.CustomerID AS [CustomerKey], 
    dg.GeographyKey AS [GeographyKey], 
    CONVERT(nvarchar(15), cu.AccountNumber) AS [CustomerAlternateKey], 
    co.Title AS [Title], 
    co.FirstName AS [FirstName], 
    co.MiddleName AS [MiddleName], 
    co.LastName AS [LastName], 
    co.NameStyle AS [NameStyle], 
    CONVERT(datetime, LEFT(Survey.ref.value(N'declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey"; 
        BirthDate',
        'varchar(20)'), 10)) AS [BirthDate], 
    Survey.ref.value(N'declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey";
        MaritalStatus',
        'nchar(1)') AS [MaritalStatus], co.Suffix AS [Suffix], 
    Survey.ref.value(N'declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey";
        Gender',
        'nvarchar(1)') AS [Gender], 
co.EmailAddress AS [EmailAddress], 
cyi.YearlyIncome AS [YearlyIncome], 
    Survey.ref.value(N'declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey";
        TotalChildren',
        'tinyint') AS [TotalChildren], 
    Survey.ref.value(N'declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey";
        NumberChildrenAtHome',
        'tinyint') AS [NumberChildrenAtHome], /*Individual Demographic XML data.*/ ifde.EnglishEducation AS [EnglishEducation], 
    ifde.SpanishEducation AS [SpanishEducation], ifde.FrenchEducation AS [FrenchEducation], ifdo.EnglishOccupation AS [EnglishOccupation], 
    ifdo.SpanishOccupation AS [SpanishOccupation], ifdo.FrenchOccupation AS [FrenchOccupation], 
    CAST(Survey.ref.value(N'declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey";
        HomeOwnerFlag',
        'bit') AS nchar(1)) AS [HouseOwnerFlag], 
    Survey.ref.value(N'declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey";
        NumberCarsOwned',
        'tinyint') AS [NumberCarsOwned], a.AddressLine1 AS [AddressLine1], a.AddressLine2 AS [AddressLine2], CONVERT(nvarchar(20), co.Phone) 
    AS [Phone], CONVERT(datetime, 
    LEFT(Survey.ref.value(N'declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey";
        DateFirstPurchase',
        'varchar(20)'), 10)) AS [DateFirstPurchase], 
    /*Individual Demographic XML data.*/ 
    Survey.ref.value(N'declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey";
        CommuteDistance',
        'nvarchar(15)') AS [CommuteDistance]
FROM [AdventureWorks].[Sales].[Customer] cu 
    INNER JOIN [AdventureWorks].[Sales].[Individual] i ON cu.CustomerID = i.CustomerID 
INNER JOIN [AdventureWorks].[Person].[Contact] co ON i.ContactID = co.ContactID 
INNER JOIN [AdventureWorks].[Sales].[CustomerAddress] ca ON cu.CustomerID = ca.CustomerID 
INNER JOIN [AdventureWorks].[Person].[Address] a ON ca.AddressID = a.AddressID 
INNER JOIN [AdventureWorks].[Person].[StateProvince] sp ON a.StateProvinceID = sp.StateProvinceID 
INNER JOIN [AdventureWorks].[Person].[CountryRegion] cr ON sp.CountryRegionCode = cr.CountryRegionCode 
INNER JOIN [dbo].[DimGeography] dg ON a.City = dg.City COLLATE SQL_Latin1_General_CP1_CS_AS 
    AND sp.StateProvinceCode = dg.StateProvinceCode COLLATE SQL_Latin1_General_CP1_CS_AS 
    AND cr.CountryRegionCode = dg.CountryRegionCode COLLATE SQL_Latin1_General_CP1_CS_AS 
    AND a.PostalCode = dg.PostalCode COLLATE SQL_Latin1_General_CP1_CS_AS 
INNER JOIN [AdventureWorks].[dbo].[tempCustomer-YearlyIncome] cyi ON cu.CustomerID = cyi.CustomerID 
INNER JOIN (SELECT DISTINCT EnglishEducation, SpanishEducation, FrenchEducation
           FROM [AdventureWorks].[dbo].[tempIndividual-ForeignData]) ifde 
    ON i.[Demographics].value(N'declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey";
        (IndividualSurvey/Education)[1]',
        'nvarchar(40)') = ifde.EnglishEducation 
INNER JOIN (SELECT DISTINCT EnglishOccupation, SpanishOccupation, FrenchOccupation
            FROM [AdventureWorks].[dbo].[tempIndividual-ForeignData]) ifdo 
    ON i.[Demographics].value(N'declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey";
        (IndividualSurvey/Occupation)[1]',
        'nvarchar(100)') = ifdo.EnglishOccupation 
CROSS APPLY i.[Demographics].nodes(N'declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey";
    IndividualSurvey') AS Survey(ref) 
WHERE ca.AddressTypeID = 2 -- Home 
ORDER BY cu.CustomerID;
~~~~

## dbo.DimDate

Replaces dbo.DimTime which was populated via Bulk Insert from DimTime.csv

## dbo.DimDepartmentGroup

~~~~SQL
SELECT 
    NULL AS ParentDepartmentGroupKey, 
    N'Corporate' AS DepartmentGroupName ;
 
SELECT DISTINCT 
    1 AS ParentDepartmentGroupKey, 
    HumanResources.Department.GroupName AS DepartmentGroupName
FROM HumanResources.Department
ORDER BY DepartmentGroupName;
~~~~

## dbo.DimEmployee

~~~~SQL
SELECT 
    NULL AS [ParentEmployeeKey], 
    e.[NationalIDNumber] AS [EmployeeNationalIDAlternateKey], 
    m.[NationalIDNumber] AS [ParentEmployeeNationalIDAlternateKey], 
    COALESCE(sp.[TerritoryID], 11) AS [SalesTerritoryKey], 
    co.[FirstName] AS [FirstName], 
    co.[LastName] AS [LastName], 
    co.[MiddleName] AS [MiddleName], 
    co.[NameStyle] AS [NameStyle], 
    e.[Title] AS [Title], 
    e.[HireDate] AS [HireDate], 
    e.[BirthDate] AS [BirthDate], 
    e.[LoginID] AS [LoginID], 
    co.[EmailAddress] AS [EmailAddress], 
    co.[Phone] AS [Phone], 
    e.[MaritalStatus] AS [MaritalStatus], 
    co.[FirstName] + N' ' + co.[LastName] AS [EmergencyContactName], 
    co.[Phone] AS [EmergencyContactPhone], 
    e.[SalariedFlag] AS [SalariedFlag], 
    e.[Gender] AS [Gender], 
    eph.[PayFrequency] AS [PayFrequency], 
    eph.[Rate] AS [BaseRate], 
    e.[VacationHours] AS [VacationHours], 
    e.[SickLeaveHours] AS [SickLeaveHours], 
    e.[CurrentFlag] AS [CurrentFlag], 
    CASE 
        WHEN sp.[SalesPersonID] IS NULL THEN 0
        ELSE 1
    END AS [SalesPersonFlag], --Set based on whether or not the Employee is in the SalesPerson table.
    d.[Name] AS [DepartmentName], 
    COALESCE(edh.[StartDate], e.[HireDate]) AS [StartDate], 
    edh.[EndDate] AS [EndDate], 
    CASE 
        WHEN edh.[EndDate] IS NULL THEN N'Current' 
        ELSE NULL
    END AS [Status] 
FROM [AdventureWorks].[HumanResources].[Employee] e 
    INNER JOIN [AdventureWorks].[Person].[Contact] co 
    ON e.[ContactID] = co.[ContactID] 
    INNER JOIN [AdventureWorks].[HumanResources].[EmployeeAddress] ea 
    ON e.[EmployeeID] = ea.[EmployeeID] 
    INNER JOIN [AdventureWorks].[Person].[Address] a 
    ON ea.[AddressID] = a.[AddressID] 
    LEFT OUTER JOIN [AdventureWorks].[Sales].[SalesPerson] sp 
    ON e.[EmployeeID] = sp.[SalesPersonID] 
    LEFT OUTER JOIN [AdventureWorks].[HumanResources].[EmployeeDepartmentHistory] edh 
    ON e.EmployeeID = edh.[EmployeeID] 
    INNER JOIN [AdventureWorks].[HumanResources].[Department] d 
    ON edh.[DepartmentID] = d.[DepartmentID] 
    LEFT OUTER JOIN [AdventureWorks].[HumanResources].[EmployeePayHistory] eph 
    ON e.EmployeeID = eph.[EmployeeID] 
    LEFT OUTER JOIN [AdventureWorks].[HumanResources].[Employee] m --Manager
    ON e.[ManagerID] = m.[EmployeeID] 
ORDER BY e.[EmployeeID];
~~~~

## dbo.DimGeography

~~~~SQL
SELECT DISTINCT 
    a.[City] AS [City], 
    sp.[StateProvinceCode] AS [StateProvinceCode], 
    sp.[Name] AS [StateProvinceName], 
    cr.[CountryRegionCode] AS [CountryRegionCode], 
    cr.[Name] AS [EnglishCountryRegionName], 
    crfn.[SpanishCountryRegionName] AS [SpanishCountryRegionName], 
    crfn.[FrenchCountryRegionName] AS [FrenchCountryRegionName], 
    a.[PostalCode] AS [PostalCode], 
    c.[TerritoryID] AS [SalesTerritoryKey] 
FROM [Person].[Address] AS a 
    INNER JOIN [Person].[StateProvince] AS sp 
    ON a.[StateProvinceID] = sp.[StateProvinceID] 
    INNER JOIN [Person].[CountryRegion] AS cr 
    ON sp.[CountryRegionCode] = cr.[CountryRegionCode] 
    INNER JOIN [Sales].[CustomerAddress] AS ca 
    ON a.[AddressID] = ca.[AddressID] 
    INNER JOIN [Sales].[Customer] AS c 
    ON ca.[CustomerID] = c.[CustomerID] 
    INNER JOIN [Sales].[SalesTerritory] AS st 
    ON c.[TerritoryID] = st.[TerritoryID] 
    LEFT OUTER JOIN [dbo].[tempCountryRegion-ForeignNames] AS crfn 
    ON sp.[CountryRegionCode] = crfn.[CountryRegionCode] 
WHERE (st.[TerritoryID] IS NOT NULL)
ORDER BY cr.[CountryRegionCode], sp.[StateProvinceCode], a.[City];
~~~~

## dbo.DimOrganization

Bulk Insert from DimOrganization.csv

## dbo.DimProduct

~~~~SQL
SELECT 
    p.ProductNumber AS ProductAlternateKey, 
    p.ProductSubcategoryID AS ProductSubcategoryKey, 
    p.WeightUnitMeasureCode AS WeightUnitMeasureCode, 
    p.SizeUnitMeasureCode AS SizeUnitMeasureCode, 
    p.[Name] AS EnglishProductName, 
    pfn.SpanishProductName AS SpanishProductName, 
    pfn.FrenchProductName AS FrenchProductName, 
    pch.StandardCost AS StandardCost, 
    p.FinishedGoodsFlag AS FinishedGoodsFlag, 
    COALESCE(p.Color, 'NA') AS Color, 
    p.SafetyStockLevel AS SafetyStockLevel, 
    p.ReorderPoint AS ReorderPoint, 
    plph.ListPrice AS ListPrice, 
    p.Size AS Size, 
    CASE 
        WHEN p.Size IN ('38', '40') THEN N'38-40 CM'
        WHEN p.Size IN ('42', '44', '46') THEN N'42-46 CM'
        WHEN p.Size IN ('48', '50', '52') THEN N'48-52 CM'
        WHEN p.Size IN ('54', '56', '58') THEN N'54-58 CM'
        WHEN p.Size IN ('60', '62') THEN N'60-62 CM'
        WHEN p.Size IN ('70') THEN N'70'
        WHEN p.Size = 'L' THEN N'L'
        WHEN p.Size = 'M' THEN N'M'
        WHEN p.Size = 'S' THEN N'S'
        WHEN p.Size = 'XL' THEN N'XL'
        WHEN p.Size IS NULL THEN N'NA'
    END AS SizeRange, 
    CONVERT(float, p.Weight) AS Weight, 
    p.DaysToManufacture AS DaysToManufacture, 
    p.ProductLine AS ProductLine, 
    0.60 * plph.ListPrice AS DealerPrice, --Column was removed from OLTP - Dealer price is calculated 60% of ListPrice
    p.Class AS Class, 
    p.Style AS Style, 
    pm.[Name] AS ModelName, --DimProduct.ModelName updated from ProductModel.[Name] through Product.ProductModelID = ProductModel.ProductModelID
    pp.LargePhoto AS LargePhoto, --Get the TOP 1 LargePhoto from ProductPhoto through ProductID
    (SELECT pd.Description FROM [AdventureWorks].Production.ProductModelProductDescriptionCulture pmpdc 
        INNER JOIN [AdventureWorks].Production.ProductDescription pd 
        ON pmpdc.ProductDescriptionID = pd.ProductDescriptionID 
    WHERE pmpdc.CultureID = 'en' 
        AND pmpdc.ProductModelID = p.ProductModelID) AS EnglishDescription, --Join on ProductModelProductDescriptionCulture.
    (SELECT pd.Description FROM [AdventureWorks].Production.ProductModelProductDescriptionCulture pmpdc 
        INNER JOIN [AdventureWorks].Production.ProductDescription pd 
        ON pmpdc.ProductDescriptionID = pd.ProductDescriptionID 
    WHERE pmpdc.CultureID = 'fr' 
        AND pmpdc.ProductModelID = p.ProductModelID) AS FrenchDescription, --Join on ProductModelProductDescriptionCulture.
    (SELECT pd.Description FROM [AdventureWorks].Production.ProductModelProductDescriptionCulture pmpdc 
        INNER JOIN [AdventureWorks].Production.ProductDescription pd 
        ON pmpdc.ProductDescriptionID = pd.ProductDescriptionID 
    WHERE pmpdc.CultureID = 'zh-cht' 
        AND pmpdc.ProductModelID = p.ProductModelID) AS ChineseDescription, --Join on ProductModelProductDescriptionCulture.
    (SELECT pd.Description FROM [AdventureWorks].Production.ProductModelProductDescriptionCulture pmpdc 
        INNER JOIN [AdventureWorks].Production.ProductDescription pd 
        ON pmpdc.ProductDescriptionID = pd.ProductDescriptionID 
    WHERE pmpdc.CultureID = 'ar' 
        AND pmpdc.ProductModelID = p.ProductModelID) AS ArabicDescription, --Join on ProductModelProductDescriptionCulture.
    (SELECT pd.Description FROM [AdventureWorks].Production.ProductModelProductDescriptionCulture pmpdc 
        INNER JOIN [AdventureWorks].Production.ProductDescription pd 
        ON pmpdc.ProductDescriptionID = pd.ProductDescriptionID 
    WHERE pmpdc.CultureID = 'he' 
        AND pmpdc.ProductModelID = p.ProductModelID) AS HebrewDescription, --Join on ProductModelProductDescriptionCulture.
    (SELECT pd.Description FROM [AdventureWorks].Production.ProductModelProductDescriptionCulture pmpdc 
        INNER JOIN [AdventureWorks].Production.ProductDescription pd 
        ON pmpdc.ProductDescriptionID = pd.ProductDescriptionID 
    WHERE pmpdc.CultureID = 'th' 
        AND pmpdc.ProductModelID = p.ProductModelID) AS ThaiDescription, --Join on ProductModelProductDescriptionCulture.
    COALESCE(plph.StartDate, pch.StartDate, p.SellStartDate) AS StartDate, --Start of the fiscal year (7/1/yyyy) based on SellStartDate?
    COALESCE(plph.EndDate, pch.EndDate, p.SellEndDate) AS EndDate, 
    CASE 
        WHEN COALESCE(plph.EndDate, pch.EndDate, p.SellEndDate) IS NULL THEN N'Current'
        ELSE NULL
    END AS Status --Need to know all the possible values and mapping for this column
FROM [AdventureWorks].Production.Product p 
    LEFT OUTER JOIN [AdventureWorks].Production.ProductModel pm 
    ON p.ProductModelID = pm.ProductModelID 
    INNER JOIN [AdventureWorks].Production.ProductProductPhoto ppp 
    ON p.ProductID = ppp.ProductID 
    INNER JOIN [AdventureWorks].Production.ProductPhoto pp 
    ON ppp.ProductPhotoID = pp.ProductPhotoID 
    LEFT OUTER JOIN [AdventureWorks].Production.ProductCostHistory pch 
    ON p.ProductID = pch.ProductID
    LEFT OUTER JOIN [AdventureWorks].Production.ProductListPriceHistory plph 
    ON p.ProductID = plph.ProductID 
        AND pch.StartDate = plph.StartDate 
        AND COALESCE(pch.EndDate, '12-31-2020') = COALESCE(plph.EndDate, '12-31-2020') 
    LEFT OUTER JOIN [AdventureWorks].[dbo].[tempProduct-ForeignNames] pfn 
    ON p.[Name] = pfn.EnglishProductName ;
~~~~

## dbo.DimProductCategory

~~~~SQL
SELECT DISTINCT 
    pc.ProductCategoryID AS ProductCategoryAlternateKey, 
    pc.[Name] AS EnglishProductCategoryName, 
    pcfn.SpanishProductCategoryName AS SpanishProductCategoryName, 
    pcfn.FrenchProductCategoryName AS FrenchProductCategoryName 
FROM [Production].[ProductCategory] pc 
    INNER JOIN [tempProductCategory-ForeignNames] pcfn 
    ON pc.[Name] = pcfn.EnglishProductCategoryName;
~~~~

## dbo.DimProductSubCategory

~~~~SQL
SELECT DISTINCT 
    ps.ProductSubcategoryID AS ProductSubcategoryKey,    
    ps.ProductSubcategoryID AS ProductSubcategoryAlternateKey, 
    ps.[Name] AS EnglishProductSubcategoryName, 
    psfn.SpanishProductSubcategoryName AS SpanishProductSubcategoryName, --Load from product translations
    psfn.FrenchProductSubcategoryName AS FrenchProductSubcategoryName, --Load from product translations
    dpc.ProductCategoryKey AS ProductCategoryKey 
FROM [AdventureWorks].[Production].[ProductSubcategory] ps 
    INNER JOIN [dbo].[DimProductCategory] dpc 
    ON ps.ProductCategoryID = dpc.ProductCategoryAlternateKey 
    LEFT OUTER JOIN [AdventureWorks]..[tempProductSubcategory-ForeignNames] psfn 
    ON ps.[Name] = psfn.EnglishProductSubcategoryName;
~~~~

## dbo.DimPromotion

~~~~SQL
SELECT DISTINCT 
    so.SpecialOfferID AS PromotionAlternateKey, 
    so.Description AS EnglishPromotionName, 
    sofd.SpanishPromotionName AS SpanishPromotionName, 
    sofd.FrenchPromotionName AS FrenchPromotionName, 
    CONVERT(float, so.DiscountPct) AS DiscountPct, 
    so.Type AS EnglishPromotionType, 
    sofd.SpanishPromotionType AS SpanishPromotionType, 
    sofd.FrenchPromotionType AS FrenchPromotionType, 
    so.Category AS EnglishPromotionCategory, 
    sofd.SpanishPromotionCategory AS SpanishPromotionCategory, 
    sofd.FrenchPromotionCategory AS FrenchPromotionCategory, 
    so.StartDate AS StartDate, 
    so.EndDate AS EndDate, 
    so.MinQty AS MinQty, 
    so.MaxQty AS MaxQty
FROM [AdventureWorks].[Sales].[SpecialOffer] AS so 
LEFT OUTER JOIN [AdventureWorks]..[tempSpecialOffer-ForeignData] AS sofd 
ON so.[Description] = sofd.[EnglishPromotionName];
~~~~

## dbo.DimReseller

~~~~SQL
SELECT 
    s.[CustomerID] AS [ResellerKey], --Unique IDENTIFIER in DW
    dg.[GeographyKey] AS [GeographyKey], --Map DW GeographyKey from related OLTP tables
    cu.[AccountNumber] AS [ResellerAlternateKey], --Where Customer.[CustomerType] = N'S'
    (SELECT TOP 1 co.[Phone] FROM [AdventureWorks].[Sales].[StoreContact] sc 
        INNER JOIN [AdventureWorks].[Person].[Contact] co ON sc.[ContactID] = co.[ContactID]
        WHERE s.[CustomerID] = sc.[CustomerID]) AS [Phone], 
    CASE Survey.[ref].[value](N'declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/StoreSurvey"; 
        (BusinessType)', N'varchar(10)') 
        WHEN N'BM' THEN N'Value Added Reseller' --Bike Manufacturer
        WHEN N'BS' THEN N'Specialty Bike Shop' 
        WHEN N'OS' THEN N'Warehouse' 
    END AS [BusinessType], --Store Demographic XML data.
    s.[Name] AS [ResellerName], 
    Survey.[ref].[value](N'declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/StoreSurvey"; 
        (NumberEmployees)', N'int') AS [NumberEmployees], --Store Demographic XML data.
    CASE Survey.[ref].[value](N'declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/StoreSurvey"; 
        (BusinessType)', N'varchar(10)') 
        WHEN N'BM' THEN N'S' --Semi-Annual
        WHEN N'BS' THEN N'A' --Annual
        WHEN N'OS' THEN N'Q' --Quarterly
    END AS [OrderFrequency], 
    (SELECT MONTH(MAX([OrderDate])) FROM [AdventureWorks].[Sales].[SalesOrderHeader] WHERE [CustomerID] = s.[CustomerID]) AS [OrderMonth], --Is this the month of the last order for this store?
    (SELECT YEAR(MIN([OrderDate])) FROM [AdventureWorks].[Sales].[SalesOrderHeader] WHERE [CustomerID] = s.[CustomerID]) AS [FirstOrderYear], --Determined based on sales?
    (SELECT YEAR(MAX([OrderDate])) FROM [AdventureWorks].[Sales].[SalesOrderHeader] WHERE [CustomerID] = s.[CustomerID]) AS [LastOrderYear], --Determined based on sales?
    Survey.[ref].[value](N'declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/StoreSurvey"; 
        (Specialty)', N'nvarchar(50)') AS [ProductLine], --Store survey "Specialty"?
    a.[AddressLine1] AS [AddressLine1], 
    a.[AddressLine2] AS [AddressLine2], 
    Survey.[ref].[value](N'declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/StoreSurvey"; 
        (AnnualSales)', N'money') AS [AnnualSales], --Store Demographic XML data.
    Survey.[ref].[value](N'declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/StoreSurvey"; 
        (BankName)', N'nvarchar(50)') AS [BankName], --Store Demographic XML data.
    mp.[MinPaymentType], --Removed from OLTP
    mp.[MinPaymentAmount], --Removed from OLTP
    Survey.[ref].[value](N'declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/StoreSurvey"; 
        (AnnualRevenue)', N'money') AS [AnnualRevenue], --Store Demographic XML data.
    Survey.[ref].[value](N'declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/StoreSurvey"; 
        (YearOpened)', N'int') AS [YearOpened] --Store Demographic XML data.
FROM [AdventureWorks].[Sales].[Customer] cu 
    INNER JOIN [AdventureWorks].[Sales].[Store] s 
    ON cu.[CustomerID] = s.[CustomerID] 
    INNER JOIN [AdventureWorks].[Sales].[CustomerAddress] ca 
    ON cu.[CustomerID] = ca.[CustomerID] 
    INNER JOIN [AdventureWorks].[Person].[Address] a 
    ON ca.[AddressID] = a.[AddressID] 
    INNER JOIN [AdventureWorks].[dbo].[tempStore-MinPayment] mp 
    ON cu.[CustomerID] = mp.[CustomerID] 
    INNER JOIN [AdventureWorks].[Person].[StateProvince] sp 
    ON a.[StateProvinceID] = sp.[StateProvinceID] 
    INNER JOIN [AdventureWorks].[Person].[CountryRegion] cr 
    ON sp.[CountryRegionCode] = cr.[CountryRegionCode] COLLATE SQL_Latin1_General_CP1_CI_AS 
    INNER JOIN [dbo].[DimGeography] dg 
    ON a.[City] = dg.[City] COLLATE SQL_Latin1_General_CP1_CI_AS 
        AND sp.[StateProvinceCode] = dg.[StateProvinceCode] COLLATE SQL_Latin1_General_CP1_CI_AS 
        AND cr.[CountryRegionCode] = dg.[CountryRegionCode] COLLATE SQL_Latin1_General_CP1_CI_AS 
        AND a.[PostalCode] = dg.[PostalCode] COLLATE SQL_Latin1_General_CP1_CI_AS 
OUTER APPLY s.[Demographics].[nodes](N'declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/StoreSurvey"; 
    /StoreSurvey') AS [Survey](ref)
WHERE ca.[AddressTypeID] = 3    -- Main Office
ORDER BY s.[Name];
~~~~

## dbo.DimSalesReason

~~~~SQL
SELECT DISTINCT 
        sr.[SalesReasonID] AS [SalesReasonKey], 
        sr.[SalesReasonID] AS [SalesReasonAlternateKey], 
        sr.[Name] AS [SalesReasonName], 
        sr.[ReasonType] AS [SalesReasonReasonType] 
FROM [AdventureWorks].[Sales].[SalesReason] sr;
~~~~

## dbo.DimSalesTerritory

~~~~SQL
SELECT 
    st.[TerritoryID] AS [SalesTerritoryKey], 
    st.[TerritoryID] AS [SalesTerritoryAlternateKey], 
    st.[Name] AS [SalesTerritoryRegion], 
    cr.[Name] AS [SalesTerritoryCountry], 
    st.[Group] AS [SalesTerritoryGroup] 
FROM [Sales].[SalesTerritory] st 
    INNER JOIN [Person].[CountryRegion] cr 
    ON st.[CountryRegionCode] = cr.[CountryRegionCode] 
ORDER BY st.[Name];

SELECT 
    0 AS [SalesTerritoryAlternateKey], 
    CONVERT(nvarchar(50), N'NA') AS [SalesTerritoryRegion], 
    CONVERT(nvarchar(50), N'NA') AS [SalesTerritoryCountry], --Need to add mapping from sales territories to Country
    CONVERT(nvarchar(50), N'NA') AS [SalesTerritoryGroup];
~~~~

## dbo.DimScenario

Bulk Insert from DimScenario.csv

## dbo.FactAdditionalInternationalProductID

## dbo.FactCallCentre

## dbo.FactCurrencyRate

~~~~SQL
SELECT 
    dc.[CurrencyKey] AS [CurrencyKey], 
    dt.[TimeKey] AS [TimeKey], --Mapped to DimTime
    CONVERT(float, 1.0000 / cr.[AverageRate]) AS [AverageRate], 
    CONVERT(float, 1.0000 / cr.[EndOfDayRate]) AS [EndOfDayRate] 
FROM [AdventureWorks].[Sales].[CurrencyRate] cr 
    INNER JOIN dbo.[DimTime] dt 
    ON cr.[CurrencyRateDate] = dt.[FullDateAlternateKey]
    INNER JOIN dbo.[DimCurrency] dc 
    ON cr.[ToCurrencyCode] = dc.[CurrencyAlternateKey] COLLATE SQL_Latin1_General_CP1_CI_AS
ORDER BY [CurrencyKey], [TimeKey];
~~~~

## dbo.FactFinance

Bulk Insert from FactFinance.csv

## dbo.FactInternetSales

~~~~SQL
SELECT 
        dp.[ProductKey] AS [ProductKey] 
        ,(SELECT [TimeKey] FROM [dbo].[DimTime] dt 
            WHERE dt.[FullDateAlternateKey] = soh.[OrderDate]) AS [OrderDateKey] 
        ,(SELECT [TimeKey] FROM [dbo].[DimTime] dt 
            WHERE dt.[FullDateAlternateKey] = soh.[DueDate]) AS [DueDateKey] 
        ,(SELECT [TimeKey] FROM [dbo].[DimTime] dt 
            WHERE dt.[FullDateAlternateKey] = soh.[ShipDate]) AS [ShipDateKey] 
        ,soh.[CustomerID] AS [CustomerKey] --DW key mapping to Customer.
        ,sod.[SpecialOfferID] AS [PromotionKey] 
        ,COALESCE(dc.[CurrencyKey], (SELECT CurrencyKey FROM [dbo].[DimCurrency] WHERE CurrencyAlternateKey = N'USD')) AS [CurrencyKey] --Updated to match OLTP which uses the RateID not the currency code.
        ,soh.[TerritoryID] AS [SalesTerritoryKey] --DW key mapping to SalesTerritory
        ,soh.[SalesOrderNumber] AS [SalesOrderNumber] 
        ,ROW_NUMBER() OVER (PARTITION BY sod.[SalesOrderID] ORDER BY sod.[SalesOrderDetailID]) AS [SalesOrderLineNumber] 
        ,soh.[RevisionNumber] AS [RevisionNumber] 
        ,sod.[OrderQty] AS [OrderQuantity] 
        ,sod.[UnitPrice] AS [UnitPrice] 
        ,sod.[OrderQty] * sod.[UnitPrice] AS [ExtendedAmount] 
        ,sod.[UnitPriceDiscount] AS [UnitPriceDiscountPct] 
        ,sod.[OrderQty] * sod.[UnitPrice] * sod.[UnitPriceDiscount] AS [DiscountAmount] 
        ,pch.[StandardCost] AS [ProductStandardCost] 
        ,sod.[OrderQty] * pch.[StandardCost] AS [TotalProductCost] 
        ,sod.[LineTotal] AS [SalesAmount] 
        ,CONVERT(money, sod.[LineTotal] * 0.08) AS [TaxAmt] --Tax amount needs to be calculated at the line item level for DW.
        ,CONVERT(money, sod.[LineTotal] * 0.025) AS [Freight] --[Freight] amount needs to be calculated at the line item level for DW.
        ,sod.[CarrierTrackingNumber] AS [CarrierTrackingNumber] 
        ,soh.[PurchaseOrderNumber] AS [CustomerPONumber] 
-- SELECT COUNT(*) 
    FROM [AdventureWorks].[Sales].[SalesOrderHeader] soh 
        INNER JOIN [AdventureWorks].[Sales].[SalesOrderDetail] sod 
        ON soh.[SalesOrderID] = sod.[SalesOrderID] 
        INNER JOIN [AdventureWorks].[Production].[Product] p 
        ON sod.[ProductID] = p.[ProductID] 
        INNER JOIN [dbo].[DimProduct] dp 
        ON dp.[ProductAlternateKey] = p.[ProductNumber] COLLATE SQL_Latin1_General_CP1_CI_AS 
            AND [dbo].[udfMinimumDate](soh.[OrderDate], soh.[DueDate]) BETWEEN dp.[StartDate] AND COALESCE(dp.[EndDate], '12-31-9999') -- Make sure we get all the Sales Orders!
        INNER JOIN [AdventureWorks].[Sales].[Customer] c 
        ON soh.[CustomerID] = c.[CustomerID] 
            AND c.[CustomerType] = N'I' -- Internet 
        LEFT OUTER JOIN [AdventureWorks].[Production].[ProductCostHistory] pch 
        ON p.[ProductID] = pch.[ProductID]
            AND [dbo].[udfMinimumDate](soh.[OrderDate], soh.[DueDate]) BETWEEN pch.[StartDate] AND COALESCE(pch.[EndDate], '12-31-9999') -- Make sure we get all the Sales Orders!
        LEFT OUTER JOIN [AdventureWorks].[Sales].[CurrencyRate] cr 
        ON soh.[CurrencyRateID] = cr.[CurrencyRateID] 
        LEFT OUTER JOIN [dbo].[DimCurrency] dc 
        ON cr.[ToCurrencyCode] = dc.[CurrencyAlternateKey] COLLATE SQL_Latin1_General_CP1_CI_AS 
        LEFT OUTER JOIN [AdventureWorks].[HumanResources].[Employee] e 
        ON soh.[SalesPersonID] = e.[EmployeeID] 
        LEFT OUTER JOIN [dbo].[DimEmployee] de 
        ON e.[NationalIDNumber] = de.[EmployeeNationalIDAlternateKey] COLLATE SQL_Latin1_General_CP1_CI_AS 
    ORDER BY [OrderDateKey], [CustomerKey];
~~~~

## dbo.FactInternetSalesReason

~~~~SQL
  SELECT
    soh.[SalesOrderNumber] AS [SalesOrderNumber], 
    ROW_NUMBER() OVER (PARTITION BY soh.[SalesOrderNumber], sohsr.[SalesReasonID] ORDER BY sohsr.[SalesReasonID]) AS [SalesOrderLineNumber], 
    sohsr.[SalesReasonID] AS [SalesReasonKey] 
FROM [AdventureWorks].[Sales].[SalesOrderHeader] soh 
    INNER JOIN [AdventureWorks].[Sales].[SalesOrderDetail] sod 
    ON soh.[SalesOrderID] = sod.[SalesOrderID]
    INNER JOIN [AdventureWorks].[Sales].[SalesOrderHeaderSalesReason] sohsr 
    ON soh.[SalesOrderID] = sohsr.[SalesOrderID] 
ORDER BY soh.[SalesOrderNumber], 2;
~~~~

## dbo.FactProductInventory

## dbo.FactResellerSales

~~~~SQL
SELECT 
    dp.[ProductKey] AS [ProductKey], 
    (SELECT [TimeKey] FROM [dbo].[DimTime] dt 
        WHERE dt.[FullDateAlternateKey] = soh.[OrderDate]) AS [OrderDateKey], 
    (SELECT [TimeKey] FROM [dbo].[DimTime] dt 
        WHERE dt.[FullDateAlternateKey] = soh.[DueDate]) AS [DueDateKey], 
    (SELECT [TimeKey] FROM [dbo].[DimTime] dt 
        WHERE dt.[FullDateAlternateKey] = soh.[ShipDate]) AS [ShipDateKey], 
    soh.[CustomerID] AS [ResellerKey], --DW key mapping to Store.
    de.[EmployeeKey] AS [EmployeeKey], --DW key mapping to Employee and SalesPerson
    sod.[SpecialOfferID] AS [PromotionKey], 
    COALESCE(dc.[CurrencyKey], (SELECT CurrencyKey FROM [dbo].[DimCurrency] WHERE CurrencyAlternateKey = N'USD')) AS [CurrencyKey], --Updated to match OLTP which uses the RateID not the currency code.
    soh.[TerritoryID] AS [SalesTerritoryKey], --DW key mapping to SalesTerritory
    soh.[SalesOrderNumber] AS [SalesOrderNumber], 
    sod.[SalesOrderDetailID] - (SELECT MIN([SalesOrderDetailID]) FROM [AdventureWorks].[Sales].[SalesOrderDetail] WHERE [SalesOrderID] = sod.[SalesOrderID] GROUP BY [SalesOrderID]) + 1 AS [SalesOrderLineNumber], 
    soh.[RevisionNumber] AS [RevisionNumber], 
    sod.[OrderQty] AS [OrderQuantity], 
    sod.[UnitPrice] AS [UnitPrice], 
    sod.[OrderQty] * sod.[UnitPrice] AS [ExtendedAmount], 
    sod.[UnitPriceDiscount] AS [UnitPriceDiscountPct], 
    sod.[OrderQty] * sod.[UnitPrice] * sod.[UnitPriceDiscount] AS [DiscountAmount], 
    pch.[StandardCost] AS [ProductStandardCost], 
    sod.[OrderQty] * pch.[StandardCost] AS [TotalProductCost], 
    sod.[LineTotal] AS [SalesAmount], 
    CONVERT(money, sod.[LineTotal] * 0.08) AS [TaxAmt], --Tax amount needs to be calculated at the line item level for DW.
    CONVERT(money, sod.[LineTotal] * 0.025) AS [Freight], --[Freight] amount needs to be calculated at the line item level for DW.
    sod.[CarrierTrackingNumber] AS [CarrierTrackingNumber], 
    soh.[PurchaseOrderNumber] AS [CustomerPONumber] 
FROM [AdventureWorks].[Sales].[SalesOrderHeader] soh 
    INNER JOIN [AdventureWorks].[Sales].[SalesOrderDetail] sod 
    ON soh.[SalesOrderID] = sod.[SalesOrderID] 
    INNER JOIN [AdventureWorks].[Production].[Product] p 
    ON sod.[ProductID] = p.[ProductID] 
    INNER JOIN [dbo].[DimProduct] dp 
    ON dp.[ProductAlternateKey] = p.[ProductNumber] COLLATE SQL_Latin1_General_CP1_CI_AS 
        AND [dbo].[udfMinimumDate](soh.[OrderDate], soh.[DueDate]) BETWEEN dp.[StartDate] AND COALESCE(dp.[EndDate], '12-31-9999') -- Make sure we get all the Sales Orders!
    INNER JOIN [AdventureWorks].[Sales].[Customer] c 
    ON soh.[CustomerID] = c.[CustomerID] 
        AND c.[CustomerType] = N'S' -- Store sales
    LEFT OUTER JOIN [AdventureWorks].[Production].[ProductCostHistory] pch 
    ON p.[ProductID] = pch.[ProductID]
        AND [dbo].[udfMinimumDate](soh.[OrderDate], soh.[DueDate]) BETWEEN pch.[StartDate] AND COALESCE(pch.[EndDate], '12-31-9999') -- Make sure we get all the Sales Orders!
    LEFT OUTER JOIN [AdventureWorks].[Sales].[CurrencyRate] cr 
    ON soh.[CurrencyRateID] = cr.[CurrencyRateID] 
    LEFT OUTER JOIN [dbo].[DimCurrency] dc 
    ON cr.[ToCurrencyCode] = dc.[CurrencyAlternateKey] COLLATE SQL_Latin1_General_CP1_CI_AS 
    LEFT OUTER JOIN [AdventureWorks].[HumanResources].[Employee] e 
    ON soh.[SalesPersonID] = e.[EmployeeID] 
    LEFT OUTER JOIN [dbo].[DimEmployee] de 
    ON e.[NationalIDNumber] = de.[EmployeeNationalIDAlternateKey] COLLATE SQL_Latin1_General_CP1_CI_AS 
ORDER BY [OrderDateKey], [ResellerKey];
~~~~

## dbo.FactSalesQuota

~~~~SQL
SELECT DISTINCT 
    spqh.[SalesPersonID] AS [EmployeeKey], 
    dt.[TimeKey] AS [TimeKey], --Map to DimTime.TimeKey 
    dt.[CalendarYear] AS [CalendarYear], 
    dt.[CalendarQuarter] AS [CalendarQuarter], 
    spqh.[SalesQuota] AS [SalesAmountQuota] 
FROM [AdventureWorks].[Sales].[SalesPersonQuotaHistory] spqh 
    INNER JOIN [dbo].[DimTime] dt 
    ON spqh.[QuotaDate] = dt.[FullDateAlternateKey];
~~~~

## dbo.FactSurveyResponse

## dbo.NewFactCurrencyRate

## dbo.ProspectiveBuyer
