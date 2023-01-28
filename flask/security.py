from functools import wraps

def admin_required(f):
    @wraps(f)
    def decorator(data, *args, **kwargs):
        
        #check if admin user
        if data[5] != 1:
            return {"message": "admin privileges are required"}, 403
        return f(data, *args, **kwargs)
    return decorator

def account_etl_required(f):
    @wraps(f)
    def decorator(data, *args, **kwargs):
        
        #check if etl user
        if data[5] != 2:
            return {"message": "Account ETL privileges are required"}, 403
        return f(data, *args, **kwargs)
    return decorator

def field_required(f):
    @wraps(f)
    def decorator(data, *args, **kwargs):
        
        #check if field user
        if data[5] != 3:
            return {"message": "Field privileges are required"}, 403
        return f(data, *args, **kwargs)
    return decorator

def agent_status_required(f):
    @wraps(f)
    def decorator(data, *args, **kwargs):
        
        #check if field user
        if data[5] != 4:
            return {"message": "Agent status privileges are required"}, 403
        return f(data, *args, **kwargs)
    return decorator

def itd_required(f):
    @wraps(f)
    def decorator(data, *args, **kwargs):
        
        #check if itd user
        if data[5] != 5:
            return {"message": "ITD privileges are required"}, 403
        return f(data, *args, **kwargs)
    return decorator
