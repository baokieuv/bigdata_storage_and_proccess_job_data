import socket
import sys

def check_port_forward():
    """Kiểm tra xem port-forward có đang chạy không"""
    
    print("\n🔍 KIỂM TRA PORT-FORWARD...")
    
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(3)
    
    try:
        result = sock.connect_ex(('localhost', 9042))
        sock.close()
        
        if result == 0:
            print("✅ Port-forward đang chạy trên localhost:9042")
            return True
        else:
            print("❌ Port-forward KHÔNG chạy!")
            print("\n💡 HÃY MỞ TERMINAL MỚI VÀ CHẠY:")
            print("   kubectl port-forward pod/cassandra-0 9042:9042")
            print("\n   Giữ terminal đó chạy, sau đó chạy lại script này")
            return False
            
    except Exception as e:
        print(f"❌ Lỗi kiểm tra port: {e}")
        sock.close()
        return False

def connect_with_verification():
    """Kết nối với verification"""
    
    if not check_port_forward():
        print("⚠️  Không thể tiếp tục vì thiếu port-forward")
        return None, None
    
    print("\n🔗 ĐANG KẾT NỐI CASSANDRA...")
    
    try:
        from cassandra.cluster import Cluster
        from cassandra.auth import PlainTextAuthProvider
        
        auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
        
        # Tăng timeout
        cluster = Cluster(
            ['localhost', '127.0.0.1'],
            port=9042,
            auth_provider=auth_provider,
            connect_timeout=15  # Tăng từ 5 lên 15 giây
        )
        
        session = cluster.connect()
        
        # Test với timeout dài hơn
        row = session.execute("SELECT release_version, cluster_name FROM system.local", timeout=10).one()
        print(f"✅ Kết nối thành công!")
        print(f"   Cassandra version: {row.release_version}")
        print(f"   Cluster name: {row.cluster_name}")
        
        return cluster, session
        
    except Exception as e:
        print(f"❌ Lỗi kết nối: {e}")
        print("\n💡 CÓ THỂ CASSANDRA POD CHƯA SẴN SÀNG:")
        print("1. Kiểm tra pod: kubectl get pods -l app=cassandra")
        print("2. Xem logs: kubectl logs cassandra-0 --tail=20")
        print("3. Đợi thêm 30 giây và thử lại")
        return None, None

def auto_wait_and_retry():
    """Tự động đợi và retry"""
    
    import time
    
    print("\n🔄 TỰ ĐỘNG ĐỢI VÀ THỬ LẠI...")
    
    for attempt in range(3):
        print(f"\n🔍 Thử lần {attempt + 1}/3...")
        
        cluster, session = connect_with_verification()
        if cluster:
            return cluster, session
        
        if attempt < 2:
            wait_time = 10 * (attempt + 1)  # 10, 20, 30 giây
            print(f"⏳ Chờ {wait_time} giây trước khi thử lại...")
            time.sleep(wait_time)
    
    print("\n❌ Đã thử hết 3 lần. Cassandra không khả dụng.")
    return None, None

# ====================== MAIN EXECUTION ======================
if __name__ == "__main__":
    print("=" * 50)
    print("🚀 BẮT ĐẦU KIỂM TRA KẾT NỐI CASSANDRA")
    print("=" * 50)
    
    # Chọn một trong các phương pháp dưới đây:
    
    # 1. Chỉ kiểm tra port-forward
    # check_port_forward()
    
    # 2. Kết nối một lần
    # cluster, session = connect_with_verification()
    
    # 3. Tự động retry (推荐)
    cluster, session = auto_wait_and_retry()
    
    if cluster:
        print("\n🎉 THÀNH CÔNG! Có thể sử dụng cluster và session để query.")
        # Ví dụ: thực hiện query
        # rows = session.execute("SELECT * FROM system_schema.keyspaces")
        # for row in rows:
        #     print(row)
        
        # Đóng kết nối khi hoàn thành
        cluster.shutdown()
        print("✅ Đã đóng kết nối Cassandra.")
    else:
        print("\n💔 KHÔNG THỂ KẾT NỐI ĐẾN CASSANDRA")
        print("Hãy kiểm tra lại các bước cài đặt.")
    
    print("\n" + "=" * 50)
    print("🏁 KẾT THÚC CHƯƠNG TRÌNH")
    print("=" * 50)