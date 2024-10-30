from pydantic import BaseModel, Field, EmailStr
from datetime import date
from typing import Optional

# Modelo para Sucursal
class SucursalCreate(BaseModel):
    nombre: str = Field(..., description="Nombre de la sucursal")
    direccion: Optional[str] = Field(None, description="Dirección de la sucursal")
    telefono: Optional[str] = Field(None, description="Teléfono de la sucursal")

class Sucursal(SucursalCreate):
    id_sucursal: int

# Modelo para Proveedor
class ProveedorCreate(BaseModel):
    nombre: str = Field(..., description="Nombre del proveedor")
    telefono: Optional[str] = Field(None, description="Teléfono del proveedor")
    correo: EmailStr = Field(..., description="Correo electrónico del proveedor")

class Proveedor(ProveedorCreate):
    id_proveedor: int

# Modelo para Cliente
class ClienteCreate(BaseModel):
    nombre: str = Field(..., description="Nombre del cliente")
    apellido: str = Field(..., description="Apellido del cliente")
    telefono: Optional[str] = Field(None, description="Teléfono del cliente")
    correo: EmailStr = Field(..., description="Correo electrónico del cliente")
    direccion: Optional[str] = Field(None, description="Dirección del cliente")

class Cliente(ClienteCreate):
    id_cliente: int

# Modelo para Empleado
class EmpleadoCreate(BaseModel):
    nombre: str = Field(..., description="Nombre del empleado")
    apellido: str = Field(..., description="Apellido del empleado")
    puesto: Optional[str] = Field(None, description="Puesto del empleado")
    telefono: Optional[str] = Field(None, description="Teléfono del empleado")
    correo: EmailStr = Field(..., description="Correo electrónico del empleado")
    id_sucursal: Optional[int] = Field(None, description="ID de la sucursal")

class Empleado(EmpleadoCreate):
    id_empleado: int

# Modelo para Producto
class ProductoCreate(BaseModel):
    marca: str = Field(..., description="Marca del vehículo")
    modelo: str = Field(..., description="Modelo del vehículo")
    ano: int = Field(..., description="Año de fabricación")
    precio: float = Field(..., description="Precio del vehículo")
    id_sucursal: Optional[int] = Field(None, description="ID de la sucursal")
    id_proveedor: Optional[int] = Field(None, description="ID del proveedor")

class Producto(ProductoCreate):
    id_producto: int

# Modelo para Venta
class VentaCreate(BaseModel):
    id_cliente: int = Field(..., description="ID del cliente")
    id_producto: int = Field(..., description="ID del producto")
    id_empleado: int = Field(..., description="ID del empleado")
    fecha_venta: date = Field(..., description="Fecha de la venta")
    cantidad: int = Field(..., description="Cantidad vendida")

class Venta(VentaCreate):
    id_venta: int
