from fastapi import APIRouter, HTTPException
from app.models import (
    SucursalCreate, Sucursal,
    ProveedorCreate, Proveedor,
    ClienteCreate, Cliente,
    EmpleadoCreate, Empleado,
    ProductoCreate, Producto,
    VentaCreate, Venta
)
from app.database import get_db_connection
from typing import List

router = APIRouter()

# Endpoints para Sucursales
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

# Endpoints para Proveedores
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