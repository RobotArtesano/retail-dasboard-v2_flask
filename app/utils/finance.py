from app import db
from app.models import Sale, Product, Inventory
from sqlalchemy import func, literal
from collections import defaultdict

def get_retail_kpis(user_id, group_by='total'):
    """
    Motor analitico central. Ajustado al modelo relacional (product_id)
    group_by puede ser: 'total', 'sku_code', 'department', 'category', 'brand', 'store'

    Realizamos dos consultas separadas: una para ventas y otra para inventario, agrupando por la dimension seleccionada.
    Luego combinamos los resultados en memoria para calcular los KPIs finales.

    """

    # 1. Determinar la columna de agrupacion dinamica
    # Si es 'total', agrupamos por un valor constante (literalmente la palabra "Total")
    if group_by == 'total':
        group_col_sales = literal("Total").label('dimension')
        group_col_inv = literal("Total").label('dimension')
    elif group_by == 'store':
        group_col_sales = Sale.store.label('dimension')
        group_col_inv = Inventory.store.label('dimension')
    else:
        # Asumimos que la agrupacion (brand, department, etc.) viene del catalogo de productos
        # Usamos getattr de forma segura por si la columna aun no existe en el modelo Product
        col_attr = getattr(Product, group_by, Product.sku_code)
        group_col_sales = col_attr.label('dimension')
        group_col_inv = col_attr.label('dimension')

    # ==========================================
    # CONSULTA 1: BLOQUE DE VENTAS Y COSTOS
    # ==========================================
    sales_query = db.session.query(
        group_col_sales,
        func.sum(Sale.qty_sold).label('venta_unidades'),
        func.sum(Sale.qty_sold * Sale.price_sale).label('venta_neta'),
        func.sum(Sale.qty_sold * Sale.unit_cost).label('costo_vendido'),
        # Venta Bruta = Cantidad vendida * Precio de lista (catalogo)
        func.sum(Sale.qty_sold * Product.price).label('venta_bruta')
    ).join(Product, Sale.product_id == Product.id
    ).filter(Sale.user_id == user_id)

    if group_by != 'total':
        sales_query = sales_query.group_by(group_col_sales)

    sales_results = sales_query.all()

    # ==========================================
    # CONSULTA 2: BLOQUE DE INVENTARIO
    # ==========================================
    inv_query = db.session.query(
        group_col_inv,
        func.sum(Inventory.inv_qty).label('inv_unidades'),
        func.sum(Inventory.inv_qty * Product.cost).label('inv_costo'),
        func.sum(Inventory.inv_qty * Product.price).label('inv_retail')
    ).join(Product, Inventory.product_id == Product.id
    ).filter(Inventory.user_id == user_id)

    if group_by != 'total':
        inv_query = inv_query.group_by(group_col_inv)

    inv_results = inv_query.all()

    # ==========================================
    # FUSIÓN Y CÁLCULO DE KPIS EN PYTHON
    # ==========================================
    # Usamos un defaultdict para unir ambas consultas basándonos en la dimensión (ej. la marca "Nike")
    merged_data = defaultdict(lambda: {
        'venta_unidades': 0,
        'venta_neta': 0.0,
        'costo_vendido': 0.0,
        'venta_bruta': 0.0,
        'inv_unidades': 0,
        'inv_costo': 0.0,
        'inv_retail': 0.0
    })

    for row in sales_results:
        dim = row.dimension or "Sin clasificar"
        merged_data[dim]['venta_unidades'] = row.venta_unidades or 0
        merged_data[dim]['venta_neta'] = row.venta_neta or 0.0
        merged_data[dim]['costo_vendido'] = row.costo_vendido or 0.0
        merged_data[dim]['venta_bruta'] = row.venta_bruta or 0

    for row in inv_results:
        dim = row.dimension or "Sin clasificar"
        merged_data[dim]['inv_unidades'] = row.inv_unidades or 0
        merged_data[dim]['inv_costo'] = row.inv_costo or 0.0
        merged_data[dim]['inv_retail'] = row.inv_retail or 0.0

    # 3. Calcular las formulas de KPIs para cada dimensión
    final_report = []

    for dimension, data in merged_data.items():
        vta_neta = data['venta_neta']
        vta_bruta = data['venta_bruta']
        cto_vendido = data['costo_vendido']
        vta_unidades = data['venta_unidades']

        inv_costo = data['inv_costo']
        inv_retail = data['inv_retail']
        inv_unidades = data['inv_unidades']

        # Cálculo de KPIs matematicas basicas Previniendo division por cero
        utilidad = vta_neta - cto_vendido
        margen_bruto = (utilidad / vta_neta * 100) if vta_neta > 0 else 0.0
        costo_promedio = (cto_vendido / vta_unidades) if vta_unidades > 0 else 0.0

        rebaja_retail = vta_bruta - vta_neta
        pct_rebaja = (rebaja_retail / vta_bruta * 100) if vta_bruta > 0 else 0.0

        margen_inv = (1 - (inv_costo / inv_retail)) * 100 if inv_retail > 0 else 0.0
        margen_inv_vendido = (1 - (cto_vendido / vta_neta)) * 100 if vta_neta > 0 else 0.0


        # DESPUES ESPECIFICAR EL PERIODO DE ANALISIS PARA CALCULAR LA ROTACION DE INVENTARIO
        rotacion_unidades = (vta_unidades / inv_unidades) if inv_unidades > 0 else "Inventario Cero"
        rotacion_costo = (cto_vendido / inv_costo) if inv_costo > 0 else "Inventario Cero"

        # Rotacion en dias
        rotacion_dias = (30.5 / rotacion_unidades) if isinstance(rotacion_unidades, (int, float)) and rotacion_unidades > 0 else "N/A"

        # Sell Through Clasico (Ventas / (Ventas + Inventario actual))
        sell_through = (vta_unidades / (vta_unidades + inv_unidades) * 100) if (vta_unidades + inv_unidades) > 0 else 0.0

        # Empaquetamos el KPI de esta fila 
        kpi_row = {
            "Dimension": dimension,
            "1_Ventas_Neta_Retail": round(vta_neta, 2),
            "2_Costo_Vendido": round(cto_vendido, 2),
            "3_Margen_Bruto_Pct": round(margen_bruto, 2),
            "4_Utilidad": round(utilidad, 2),
            "5_Costo_Promedio": round(costo_promedio, 2),
            "6_Venta_Bruta_Retail": round(vta_bruta, 2),
            "7_Rebaja_Retail": round(rebaja_retail, 2),
            "8_Pct_Rebaja": round(pct_rebaja, 2),
            "9_Margen_Inventario_Pct": round(margen_inv, 2),
            "10_MIV_Pct": round(margen_inv_vendido, 2),
            "13_Rotacion_Unidades": round(rotacion_unidades, 2) if isinstance(rotacion_unidades, float) else rotacion_unidades,
            "14_Rotacion_Costo": round(rotacion_costo, 2) if isinstance(rotacion_costo, float) else rotacion_costo,
            "15_Dias_Rotacion": round(rotacion_dias, 2) if isinstance(rotacion_dias, float) else rotacion_dias,
            "16_Sell_Through_Pct": round(sell_through, 2),
            
            # --- INDICADORES CON INFORMACIÓN FALTANTE ---
            "11_Desplazado_Unidades": "Falta Info: Módulo de Pedidos Pendientes no activo.",
            "12_Desplazado_Dinero": "Falta Info: Módulo de Pedidos Pendientes no activo.",
            "17_Pct_Agotado": "Falta Info: Requiere Maestro de Tiendas Activas.",
            "18_Dias_Inventario_IOH": "Falta Info: Requiere Cerebro Predictivo (Fase 4)."
        }
        final_report.append(kpi_row)

    return final_report
        
