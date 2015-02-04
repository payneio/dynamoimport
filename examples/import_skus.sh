#!/bin/bash
../dynamoimport -writes=1000 \
           -table=skus \
           -mapping=SKU,Size1,Size2,Style,OriginalRetail,RecommendRetail,DivisionCode,SubdivisionCode,SubdivisionName,DepartmentCode,DepartmentName,ClassCode,ClassName,SubclassCode,SubclassName,Description,LongDescription,Color,ColorCode,SupplierCode,SupplierName,VendorProductNumber \
           -test \
           sample_skus.data
