from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from datetime import date
from app.database import get_db_connection
from app.models import *

router = APIRouter()
@router.post("/sucursales/", response_model=Sucursal, tags=["Sucursales"])
def crear_sucursal(sucursal: SucursalCreate):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        query = """
        INSERT INTO sucursales (nombre, direccion, telefono)
        VALUES (%s, %s, %s)
        """
        values = (sucursal.nombre, sucursal.direccion, sucursal.telefono)
        cursor.execute(query, values)
        conn.commit()
        sucursal_id = cursor.lastrowid
        return Sucursal(id_sucursal=sucursal_id, **sucursal.dict())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

@router.get("/sucursales/", response_model=List[Sucursal], tags=["Sucursales"])
def listar_sucursales():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    try:
        query = "SELECT * FROM sucursales"
        cursor.execute(query)
        sucursales = cursor.fetchall()
        return [Sucursal(**sucursal) for sucursal in sucursales]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

@router.post("/sucursales/bulk/", response_model=List[Sucursal], tags=["Sucursales"])
def crear_sucursales_bulk(sucursales: List[SucursalCreate]):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        created_sucursales = []
        for sucursal in sucursales:
            query = """
            INSERT INTO sucursales (nombre, direccion, telefono)
            VALUES (%s, %s, %s)
            """
            values = (sucursal.nombre, sucursal.direccion, sucursal.telefono)
            cursor.execute(query, values)
            sucursal_id = cursor.lastrowid
            created_sucursales.append(Sucursal(id_sucursal=sucursal_id, **sucursal.dict()))
        
        conn.commit()
        return created_sucursales
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

@router.post("/proveedores/", response_model=Proveedor, tags=["Proveedores"])
def crear_proveedor(proveedor: ProveedorCreate):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        query = """
        INSERT INTO proveedores (nombre, telefono, correo)
        VALUES (%s, %s, %s)
        """
        values = (proveedor.nombre, proveedor.telefono, proveedor.correo)
        cursor.execute(query, values)
        conn.commit()
        proveedor_id = cursor.lastrowid
        return Proveedor(id_proveedor=proveedor_id, **proveedor.dict())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

@router.get("/proveedores/", response_model=List[Proveedor], tags=["Proveedores"])
def listar_proveedores():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    try:
        query = "SELECT * FROM proveedores"
        cursor.execute(query)
        proveedores = cursor.fetchall()
        return [Proveedor(**proveedor) for proveedor in proveedores]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

@router.post("/proveedores/bulk/", response_model=List[Proveedor], tags=["Proveedores"])
def crear_proveedores_bulk(proveedores: List[ProveedorCreate]):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        created_proveedores = []
        for proveedor in proveedores:
            query = """
            INSERT INTO proveedores (nombre, telefono, correo)
            VALUES (%s, %s, %s)
            """
            values = (proveedor.nombre, proveedor.telefono, proveedor.correo)
            cursor.execute(query, values)
            proveedor_id = cursor.lastrowid
            created_proveedores.append(Proveedor(id_proveedor=proveedor_id, **proveedor.dict()))
        
        conn.commit()
        return created_proveedores
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

@router.post("/clientes/", response_model=Cliente, tags=["Clientes"])
def crear_cliente(cliente: ClienteCreate):
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        query = """
        INSERT INTO clientes (nombre, apellido, telefono, correo, direccion)
        VALUES (%s, %s, %s, %s, %s)
        """
        values = (cliente.nombre, cliente.apellido, cliente.telefono, cliente.correo, cliente.direccion)
        cursor.execute(query, values)
        cliente_id = cursor.lastrowid
        conn.commit()
        return Cliente(id_cliente=cliente_id, **cliente.dict())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

@router.get("/clientes/", response_model=List[Cliente], tags=["Clientes"])
def listar_clientes():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    try:
        query = "SELECT * FROM clientes"
        cursor.execute(query)
        clientes = cursor.fetchall()
        return [Cliente(**cliente) for cliente in clientes]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()


@router.post("/clientes/bulk/", response_model=List[Cliente], tags=["Clientes"])
def crear_clientes_bulk(clientes: List[ClienteCreate]):
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        created_clientes = []
        for cliente in clientes:
            query = """
            INSERT INTO clientes (nombre, apellido, telefono, correo, direccion)
            VALUES (%s, %s, %s, %s, %s)
            """
            values = (cliente.nombre, cliente.apellido, cliente.telefono, cliente.correo, cliente.direccion)
            cursor.execute(query, values)
            cliente_id = cursor.lastrowid
            created_clientes.append(Cliente(id_cliente=cliente_id, **cliente.dict()))

        conn.commit()
        return created_clientes
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()


@router.post("/empleados/", response_model=Empleado, tags=["Empleados"])
def crear_empleado(empleado: EmpleadoCreate):
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        query = """
        INSERT INTO empleados (nombre, apellido, puesto, telefono, correo, id_sucursal)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        values = (empleado.nombre, empleado.apellido, empleado.puesto, empleado.telefono, empleado.correo, empleado.id_sucursal)
        cursor.execute(query, values)
        empleado_id = cursor.lastrowid
        conn.commit()
        return Empleado(id_empleado=empleado_id, **empleado.dict())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

@router.get("/empleados/", response_model=List[Empleado], tags=["Empleados"])
def listar_empleados():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    try:
        query = "SELECT * FROM empleados"
        cursor.execute(query)
        empleados = cursor.fetchall()
        return [Empleado(**empleado) for empleado in empleados]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

@router.post("/empleados/bulk/", response_model=List[Empleado], tags=["Empleados"])
def crear_empleados_bulk(empleados: List[EmpleadoCreate]):
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        created_empleados = []
        for empleado in empleados:
            query = """
            INSERT INTO empleados (nombre, apellido, puesto, telefono, correo, id_sucursal)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            values = (empleado.nombre, empleado.apellido, empleado.puesto, empleado.telefono, empleado.correo, empleado.id_sucursal)
            cursor.execute(query, values)
            empleado_id = cursor.lastrowid
            created_empleados.append(Empleado(id_empleado=empleado_id, **empleado.dict()))

        conn.commit()
        return created_empleados
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()


@router.post("/productos/", response_model=Producto, tags=["Productos"])
def crear_producto(producto: ProductoCreate):
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        query = """
        INSERT INTO productos (marca, modelo, ano, precio, id_sucursal, id_proveedor)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        values = (producto.marca, producto.modelo, producto.ano, producto.precio, producto.id_sucursal, producto.id_proveedor)
        cursor.execute(query, values)
        producto_id = cursor.lastrowid
        conn.commit()
        return Producto(id_producto=producto_id, **producto.dict())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

@router.get("/productos/", response_model=List[Producto], tags=["Productos"])
def listar_productos():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    try:
        query = "SELECT * FROM productos"
        cursor.execute(query)
        productos = cursor.fetchall()
        return [Producto(**producto) for producto in productos]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

@router.post("/productos/bulk/", response_model=List[Producto], tags=["Productos"])
def crear_productos_bulk(productos: List[ProductoCreate]):
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        created_productos = []
        for producto in productos:
            query = """
            INSERT INTO productos (marca, modelo, ano, precio, id_sucursal, id_proveedor)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            values = (producto.marca, producto.modelo, producto.ano, producto.precio, producto.id_sucursal, producto.id_proveedor)
            cursor.execute(query, values)
            producto_id = cursor.lastrowid
            created_productos.append(Producto(id_producto=producto_id, **producto.dict()))

        conn.commit()
        return created_productos
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

@router.get("/ventas/", response_model=List[Venta], tags=["Ventas"])
def listar_ventas():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    try:
        query = "SELECT * FROM ventas"
        cursor.execute(query)
        ventas = cursor.fetchall()
        return [Venta(**venta) for venta in ventas]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

@router.post("/ventas/", response_model=Venta, tags=["Ventas"])
def crear_venta(venta: VentaCreate):
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        query = """
        INSERT INTO ventas (id_cliente, id_producto, id_empleado, fecha_venta, cantidad)
        VALUES (%s, %s, %s, %s, %s)
        """
        values = (venta.id_cliente, venta.id_producto, venta.id_empleado, venta.fecha_venta, venta.cantidad)
        cursor.execute(query, values)
        venta_id = cursor.lastrowid
        conn.commit()
        return Venta(id_venta=venta_id, **venta.dict())
    except Exception as e:
        print(f"Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()


@router.post("/ventas/bulk/", response_model=List[Venta], tags=["Ventas"])
def crear_ventas_bulk(ventas: List[VentaCreate]):
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        created_ventas = []
        for venta in ventas:
            query = """
            INSERT INTO ventas (id_cliente, id_producto, id_empleado, fecha_venta, cantidad)
            VALUES (%s, %s, %s, %s, %s)
            """
            values = (venta.id_cliente, venta.id_producto, venta.id_empleado, venta.fecha_venta, venta.cantidad)
            cursor.execute(query, values)
            venta_id = cursor.lastrowid
            created_ventas.append(Venta(id_venta=venta_id, **venta.dict()))

        conn.commit()
        return created_ventas
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()  

        conn.close()      


@router.get("/query1/", tags=["Query1 ventas por sucursal"])
async def ventas_por_sucursal():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    query = """
    SELECT 
        s.nombre as sucursal,
        COUNT(v.id_venta) as total_ventas,
        AVG(p.precio) as precio_promedio
    FROM sucursales s
    LEFT JOIN productos p ON s.id_sucursal = p.id_sucursal
    LEFT JOIN ventas v ON p.id_producto = v.id_producto
    GROUP BY s.id_sucursal
    """
    cursor.execute(query)
    return cursor.fetchall()

@router.get("/query2/", tags=["Query2 productos por precio"])
async def productos_por_precio(
    precio_min: float = Query(..., description="Precio mínimo"),
    precio_max: float = Query(..., description="Precio máximo")
):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    query = """
    SELECT p.*, s.nombre as sucursal, pr.nombre as proveedor
    FROM productos p
    INNER JOIN sucursales s ON p.id_sucursal = s.id_sucursal
    INNER JOIN proveedores pr ON p.id_proveedor = pr.id_proveedor
    WHERE p.precio BETWEEN %s AND %s
    """
    cursor.execute(query, (precio_min, precio_max))
    return cursor.fetchall()

@router.get("/query3/", tags=["Query3 vendedores"])
async def top_vendedores(
    limite: int = Query(5, description="Cantidad de vendedores a mostrar")
):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    query = """
    SELECT 
        e.nombre,
        e.apellido,
        s.nombre as sucursal,
        COUNT(v.id_venta) as total_ventas,
        SUM(p.precio * v.cantidad) as valor_total_ventas
    FROM empleados e
    INNER JOIN ventas v ON e.id_empleado = v.id_empleado
    INNER JOIN productos p ON v.id_producto = p.id_producto
    LEFT JOIN sucursales s ON e.id_sucursal = s.id_sucursal
    GROUP BY e.id_empleado
    ORDER BY total_ventas DESC
    LIMIT %s
    """
    cursor.execute(query, (limite,))
    return cursor.fetchall()

@router.get("/query4/{id_cliente}", tags=["Query4 historial compras cliente"])
async def historial_compras_cliente(id_cliente: int):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    query = """
    SELECT 
        v.fecha_venta,
        p.marca,
        p.modelo,
        p.precio,
        v.cantidad,
        (p.precio * v.cantidad) as total,
        e.nombre as vendedor,
        s.nombre as sucursal
    FROM ventas v
    INNER JOIN productos p ON v.id_producto = p.id_producto
    INNER JOIN empleados e ON v.id_empleado = e.id_empleado
    INNER JOIN sucursales s ON p.id_sucursal = s.id_sucursal
    WHERE v.id_cliente = %s
    ORDER BY v.fecha_venta DESC
    """
    cursor.execute(query, (id_cliente,))
    return cursor.fetchall()

@router.get("/query5/{id_sucursal}", tags=["Query5 ventas por sucursal"])
async def inventario_sucursal(id_sucursal: int):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    query = """
    SELECT 
        p.*,
        pr.nombre as proveedor,
        COUNT(v.id_venta) as ventas_realizadas
    FROM productos p
    LEFT JOIN proveedores pr ON p.id_proveedor = pr.id_proveedor
    LEFT JOIN ventas v ON p.id_producto = v.id_producto
    WHERE p.id_sucursal = %s
    GROUP BY p.id_producto
    """
    cursor.execute(query, (id_sucursal,))
    return cursor.fetchall()

@router.get("/query6/", tags=["Query6 productos mas vendidos"])
async def productos_mas_vendidos(
    fecha_inicio: date = Query(..., description="Fecha inicial"),
    fecha_fin: date = Query(..., description="Fecha final"),
    limite: int = Query(10, description="Cantidad de productos a mostrar")
):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    query = """
    SELECT 
        p.marca,
        p.modelo,
        SUM(v.cantidad) as unidades_vendidas,
        SUM(p.precio * v.cantidad) as ingresos_totales,
        s.nombre as sucursal
    FROM productos p
    INNER JOIN ventas v ON p.id_producto = v.id_producto
    LEFT JOIN sucursales s ON p.id_sucursal = s.id_sucursal
    WHERE v.fecha_venta BETWEEN %s AND %s
    GROUP BY p.id_producto
    ORDER BY unidades_vendidas DESC
    LIMIT %s
    """
    cursor.execute(query, (fecha_inicio, fecha_fin, limite))
    return cursor.fetchall()

@router.get("/query7/", tags=["Query7 rendimiento sucursales"])
async def rendimiento_sucursales():    
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    query = """
    SELECT 
        s.nombre as sucursal,
        COUNT(DISTINCT e.id_empleado) as total_empleados,
        COUNT(DISTINCT v.id_venta) as total_ventas,
        SUM(p.precio * v.cantidad) as ingresos_totales,
        AVG(p.precio * v.cantidad) as ticket_promedio
    FROM sucursales s
    LEFT JOIN empleados e ON s.id_sucursal = e.id_sucursal
    LEFT JOIN ventas v ON e.id_empleado = v.id_empleado
    LEFT JOIN productos p ON v.id_producto = p.id_producto
    GROUP BY s.id_sucursal
    ORDER BY ingresos_totales DESC
    """
    cursor.execute(query)    
    return cursor.fetchall()

@router.get("/query8/", tags=["Query8 analisis proveedores"])
async def analisis_proveedores():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    query = """
    SELECT 
        pr.nombre as proveedor,
        COUNT(p.id_producto) as total_productos,
        AVG(p.precio) as precio_promedio,
        MIN(p.precio) as precio_minimo,
        MAX(p.precio) as precio_maximo,
        COUNT(v.id_venta) as total_ventas
    FROM proveedores pr
    LEFT JOIN productos p ON pr.id_proveedor = p.id_proveedor
    LEFT JOIN ventas v ON p.id_producto = v.id_producto
    GROUP BY pr.id_proveedor
    ORDER BY total_ventas DESC
    """
    cursor.execute(query)
    return cursor.fetchall()

@router.get("/query9/", tags=["Query9 clientes frecuentes"])
async def clientes_frecuentes(
    min_compras: int = Query(3, description="Mínimo de compras para considerar cliente frecuente")
):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    query = """
    SELECT 
        c.nombre,
        c.apellido,
        COUNT(v.id_venta) as total_compras,
        SUM(p.precio * v.cantidad) as total_gastado,
        MAX(v.fecha_venta) as ultima_compra
    FROM clientes c
    INNER JOIN ventas v ON c.id_cliente = v.id_cliente
    INNER JOIN productos p ON v.id_producto = p.id_producto
    GROUP BY c.id_cliente
    HAVING total_compras >= %s
    ORDER BY total_compras DESC
    """
    cursor.execute(query, (min_compras,))
    return cursor.fetchall()

@router.get("/query10/", tags=["Query10 productos por año"])
async def productos_por_ano(
    ano: int = Query(..., description="Año del modelo")
):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    query = """
    SELECT 
        p.*,
        s.nombre as sucursal,
        pr.nombre as proveedor,
        COUNT(v.id_venta) as ventas_realizadas
    FROM productos p
    LEFT JOIN sucursales s ON p.id_sucursal = s.id_sucursal
    LEFT JOIN proveedores pr ON p.id_proveedor = pr.id_proveedor
    LEFT JOIN ventas v ON p.id_producto = v.id_producto
    WHERE p.ano = %s
    GROUP BY p.id_producto
    """
    cursor.execute(query, (ano,))
    return cursor.fetchall()

@router.get("/query11/", tags=["Query11 ventas periodo sucursal"])
async def ventas_periodo_sucursal(
    fecha_inicio: date = Query(..., description="Fecha inicial"),
    fecha_fin: date = Query(..., description="Fecha final"),
    id_sucursal: Optional[int] = Query(None, description="ID de la sucursal (opcional)")
):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    base_query = """
    SELECT 
        s.nombre as sucursal,
        DATE_FORMAT(v.fecha_venta, '%Y-%m') as mes,
        COUNT(v.id_venta) as total_ventas,
        SUM(p.precio * v.cantidad) as ingresos_totales
    FROM sucursales s
    LEFT JOIN productos p ON s.id_sucursal = p.id_sucursal
    LEFT JOIN ventas v ON p.id_producto = v.id_producto
    WHERE v.fecha_venta BETWEEN %s AND %s
    """
    
    params = [fecha_inicio, fecha_fin]
    if id_sucursal:
        base_query += " AND s.id_sucursal = %s"
        params.append(id_sucursal)
    
    base_query += " GROUP BY s.id_sucursal, mes ORDER BY mes, s.nombre"
    
    cursor.execute(base_query, tuple(params))
    return cursor.fetchall()

@router.get("/query12/", tags=["Query12 empleados y ventas"])
async def empleados_ventas():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    query = """
    SELECT 
        e.nombre,
        e.apellido,
        s.nombre as sucursal,
        COUNT(v.id_venta) as total_ventas,
        MAX(v.fecha_venta) as fecha_ultima_venta
    FROM empleados e
    LEFT JOIN sucursales s ON e.id_sucursal = s.id_sucursal
    LEFT JOIN ventas v ON e.id_empleado = v.id_empleado
    GROUP BY e.id_empleado, e.nombre, e.apellido, s.nombre
    ORDER BY total_ventas ASC
    """
    cursor.execute(query)
    return cursor.fetchall()

@router.get("/query13/", tags=["Query13 productos sin movimiento"])
async def productos_sin_movimiento():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    query = """
    SELECT 
        p.*,
        s.nombre as sucursal,
        pr.nombre as proveedor
    FROM productos p
    LEFT JOIN sucursales s ON p.id_sucursal = s.id_sucursal
    LEFT JOIN proveedores pr ON p.id_proveedor = pr.id_proveedor
    LEFT JOIN ventas v ON p.id_producto = v.id_producto
    WHERE v.id_venta IS NULL
    ORDER BY p.precio DESC
    """
    cursor.execute(query)
    return cursor.fetchall()

@router.get("/query14/", tags=["Query14 ventas por marca"])
async def ventas_por_marca(
    ano: Optional[int] = Query(None, description="Año específico (opcional)")
):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    query = """
    SELECT 
        p.marca,
        COUNT(v.id_venta) as total_ventas,
        SUM(v.cantidad) as unidades_vendidas,
        AVG(p.precio) as precio_promedio,
        SUM(p.precio * v.cantidad) as ingresos_totales
    FROM productos p
    LEFT JOIN ventas v ON p.id_producto = v.id_producto
    WHERE 1=1
    """
    
    params = []
    if ano:
        query += " AND YEAR(v.fecha_venta) = %s"
        params.append(ano)
    
    query += " GROUP BY p.marca ORDER BY total_ventas DESC"
    
    cursor.execute(query, tuple(params) if params else None)
    return cursor.fetchall()

@router.get("/query15/", tags=["Query15 eficiencia vendedores"])
async def eficiencia_vendedores(
    fecha_inicio: date = Query(..., description="Fecha inicial"),
    fecha_fin: date = Query(..., description="Fecha final")
):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    query = """
    SELECT 
        s.nombre as sucursal,
        e.nombre,
        e.apellido,
        COUNT(v.id_venta) as total_ventas,
        SUM(p.precio * v.cantidad) as ingresos_totales,
        AVG(p.precio * v.cantidad) as ticket_promedio,
        COUNT(v.id_venta) / 
            DATEDIFF(%s, %s) as ventas_por_dia
    FROM empleados e
    INNER JOIN sucursales s ON e.id_sucursal = s.id_sucursal
    LEFT JOIN ventas v ON e.id_empleado = v.id_empleado
    LEFT JOIN productos p ON v.id_producto = p.id_producto
    WHERE v.fecha_venta BETWEEN %s AND %s
    GROUP BY e.id_empleado
    ORDER BY ventas_por_dia DESC
    """
    cursor.execute(query, (fecha_fin, fecha_inicio, fecha_inicio, fecha_fin))
    return cursor.fetchall()