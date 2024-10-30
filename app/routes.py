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